use std::collections::BTreeMap;
use std::fs::File;
use std::io::prelude::*;
//use std::net::TcpStream;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::message::*;
pub struct RDB {
    pub _storage: BTreeMap<String, Item>,
    pub _db_selector: usize,
}

impl RDB {
    fn read_header(&mut self, s: &[u8]) -> Option<usize> {
        let mut probe = 0;
        while s[probe] != 0xFE && probe < s.len() {
            probe += 1
        }
        println!("skipping {} bytes", probe);
        // TODO: Implementing Boundary/Validity check
        self._db_selector = s[probe + 1] as usize;
        Some(probe + 5)
    }
    fn read_data(&mut self, s: &[u8], index: usize) -> Option<usize> {
        // Validity Check
        if index >= s.len() {
            return None;
        }
        // DataSegment Terminal Check
        if s[index] == 0xFF {
            return None;
        }

        let mut index = index;
        let mut item = Item {
            value: "".to_string(),
            expire: 0,
        };
        // Check Expire Timestamp
        if s[index] == 0xFD {
            // 0xFD leads to 4 byte uint timestamp in seconds
            item.expire =
                (u32::from_le_bytes(s[index + 1..index + 5].try_into().unwrap()) * 1000) as u64;
            index += 5;
        } else if s[index] == 0xFC {
            // 0xFC leads to 8 bytes uint timestamp in miliseconds
            item.expire = u64::from_le_bytes(s[index + 1..index + 9].try_into().unwrap());
            index += 9;
        }

        // TODO: Implement check on other type of data
        let mut index = index + 1;

        // Still need to read the stream even when the data is marked expired
        let key;
        if let Some((nindex, length)) = self.parse_length_encoding(s, index) {
            println!("Reading from {} to {}", nindex, nindex + length);
            key = String::from_utf8(s[nindex..nindex + length].to_vec()).unwrap();
            println!("new key {}", key);
            index = nindex + length;
        } else {
            return None;
        }
        if let Some((nindex, length)) = self.parse_length_encoding(s, index) {
            println!("Reading from {} to {}", nindex, nindex + length);
            item.value = String::from_utf8(s[nindex..nindex + length].to_vec()).unwrap();
            println!("new value {}", item.value);

            // Discard expired data here
            if item.expire != 0
                && item.expire
                    < SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64
            {
                println!("Value expired");
            } else {
                self._storage.insert(key, item);
            }
            Some(nindex + length)
        } else {
            None
        }
    }
    fn parse_length_encoding(&mut self, s: &[u8], index: usize) -> Option<(usize, usize)> {
        if index >= s.len() {
            return None;
        }
        // TODO: Right now only implementing length-encoding case one
        if s[index] < 64 {
            return Some((index + 1, s[index] as usize));
        }
        //else if s[0] < 128 {
        //    return Some((&s[2..], (s[0] % 64 * 256 + s[1]) as usize));
        //}
        None
    }
    pub fn read_rdb_from_file(dbfilename: String) -> Self {
        let path = Path::new(&dbfilename);
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(_err) => {
                return RDB {
                    _db_selector: 0,
                    _storage: BTreeMap::new(),
                }
            }
        };
        let mut data = vec![];
        if file.read_to_end(&mut data).is_ok() {
            println!("Reading {} bytes from rdb file", &data.len());
        }
        let data: &[u8] = &data;
        let mut rdb = RDB {
            _storage: BTreeMap::new(),
            _db_selector: 0,
        };
        let mut res = rdb.read_header(data);
        while let Some(index) = res {
            res = rdb.read_data(data, index);
        }
        rdb
    }
    //pub fn read_rdb_from_stream(stream: &TcpStream) -> Self {}

    pub fn fullresync_rdb() -> Vec<u8> {
        let rdb = [
            0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64,
            0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a,
            0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05,
            0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73,
            0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61,
            0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe,
            0xc0, 0xff, 0x5a, 0xa2,
        ];
        [format!("${}\r\n", &rdb.len()).as_bytes(), &rdb].concat()
    }
}
