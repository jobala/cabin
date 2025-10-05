use std::u8;

use bytes::{Buf, BufMut, Bytes};

pub(crate) mod builder;
pub(crate) mod iterator;

/// Block encoding
/// ----------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |      Extra      |
/// ----------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
///----------------------------------------------------------------------------------------------------
pub(crate) struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

const SIZEOF_U16: usize = 2;

impl Block {
    fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();

        for offset in self.offsets.clone() {
            buf.put_u16(offset);
        }

        buf.put_u16(self.offsets.len() as u16);
        buf.into()
    }

    fn decode(data: &[u8]) -> Self {
        let extra_start = data.len() - SIZEOF_U16;
        let num_of_entries = (&data[(data.len() - SIZEOF_U16)..]).get_u16() as usize;
        let offset_start = extra_start - (SIZEOF_U16 * num_of_entries);

        let offsets = &data[offset_start..extra_start]
            .chunks(2)
            .map(|mut x| x.get_u16())
            .collect::<Vec<u16>>();

        let data = &data[0..offset_start];

        Block {
            data: data.to_vec(),
            offsets: offsets.clone(),
        }
    }
}
