// Copyright 2020 Ant Group. All rights reserved.
// Copyright (C) 2020 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::io::{Read, Write, Result};
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::metadata::layout::v5::{RafsBlobEntry, RafsChunkFlags, RafsChunkInfo, RafsV5BlobTable};
use crate::metadata::{Inode, RafsInode};

use nydus_utils::digest::RafsDigest;

pub struct DedupState {
    inodes: HashMap<Inode, Arc<DedupInode>>,
}

impl DedupState {
    pub fn new() -> Self {
        DedupState {
            inodes: HashMap::new(),
        }
    }

    pub fn process(&mut self, sockpath: &String, sid: &String, inodes: Vec<Arc<dyn RafsInode>>, blobs: Vec<Arc<RafsBlobEntry>>) -> Result<()> {
        if inodes.len() <= 0 {
            return Ok(());
        }

        let mut client = DedupClient::connect(sockpath)?;
        let mut blob_tables: HashMap<String, Arc<RafsV5BlobTable>> = HashMap::new();
        for inode in inodes {
            let inode_hash = inode.get_digest().to_string();
            let get_inode_resp = client.get_inode(&inode_hash)?;
            if get_inode_resp.status == "ok" {
                if get_inode_resp.message == Some("not found".into()) {
                    let child_count = inode.get_child_count();
                    let mut chunks = Vec::new();
                    for idx in 0..child_count {
                        let chunk = inode.get_chunk_info(idx)?;
                        let chunk_data = ChunkData {
                            blob_index: chunk.blob_index(),
                            index: chunk.index(),
                            compress_offset: chunk.compress_offset(),
                            decompress_offset: chunk.decompress_offset(),
                        };
                        chunks.push(chunk_data);
                    }
                    let inode_data = InodeData {
                        id: sid.clone(),
                        data: chunks,
                    };
                    let _put_inode_resp = client.put_inode(&inode_hash, &inode_data)?;
                } else {
                    let inode_data: InodeData = serde_json::from_value(get_inode_resp.data.unwrap())?;
                    if inode_data.id == *sid {
                        continue;
                    }
                    let blob_table = if let Some(table) = blob_tables.get(&inode_data.id) {
                        table.clone()
                    } else {
                        let get_blob_resp = client.get_blob(&inode_data.id)?;
                        if get_blob_resp.message == Some("not found".into()) {
                            continue;
                        }
                        let blob_data: BlobTableData = serde_json::from_value(get_blob_resp.data.unwrap())?;

                        let blob_table = Arc::new(RafsV5BlobTable::new_from(&blob_data));
                        blob_tables.insert(sid.clone(), blob_table.clone());
                        blob_table
                    };

                    let dedup_inode = Arc::new(DedupInode::new(&inode_data, inode.as_ref(), blob_table));
                    self.inodes.insert(inode.ino(), dedup_inode);
                }
            } else {
                info!("fail to get inode");
            }
        }

        let entries = blobs.iter().map(|blob| {
            BlobEntryData {
                chunk_count: blob.chunk_count,
                readahead_offset: blob.readahead_offset,
                readahead_size: blob.readahead_size,
                blob_id: blob.blob_id.clone(),
                blob_index: blob.blob_index,
                blob_cache_size: blob.blob_cache_size,
                compressed_blob_size: blob.compressed_blob_size,
            }
        }).collect();

        let blob_data = BlobTableData {
            entries: entries
        };

        let _put_blob_resp = client.put_blob(sid, &blob_data)?;
        Ok(())
    }

    // creat cached dedup info for v5 version
    // pub fn load_inode(&mut self, ino: Inode, r: &mut RafsIoReader) -> Result<()> {
    //     let mut sb = RafsV5SuperBlock::new();
    //     r.read_exact(sb.as_mut())?;
    //     // TODO: judge version, dont dedplicate for v4
        
    //     let inodes_count = sb.inodes_count();
    //     let inode_table_offset = sb.inode_table_offset();
    //     let inode_table_offset = sb.inode_table_offset();
    //     let blob_table_offset = sb.blob_table_offset();
    //     let blob_table_size = sb.blob_table_size();

    //     // get blob table
    //     // we dont need extended blob table for dedup so store blob table only
    //     r.seek(SeekFrom::Start(blob_table_offset))?;
    //     let mut blob_table = RafsV5BlobTable::new();
    //     blob_table.load(r, blob_table_size)?;

    //     // get inode
    //     if ino == 0 || ino > inodes_count {
    //         return Err(enoent!());
    //     }
    //     r.seek(SeekFrom::Start(inode_table_offset + (ino - 1) as u64 * size_of::<u32>() as u64))?;
    //     let mut offset = [0u8; size_of::<u32>()];
    //     r.read_exact(&mut offset)?;
    //     let inode_offset = u32::from_le_bytes(offset) << 3;
    //     r.seek(SeekFrom::Start(inode_offset as u64))?;
    //     let mut inode = DedupInode::new(Arc::new(blob_table));
    //     inode.load(r);
    //     self.inodes.insert(inode.ino, inode);
    // }


    pub fn get_inode(&self, ino: Inode) -> Option<Arc<DedupInode>> {
        self.inodes.get(&ino).cloned()
    }
}


#[derive(Default, Clone, Debug)]
pub struct DedupInode {
    data: Vec<Arc<DedupChunkInfo>>,
    blob_table: Arc<RafsV5BlobTable>,
}

impl DedupInode {
    pub fn new(inode_data: &InodeData, inode: &dyn RafsInode, blob_table: Arc<RafsV5BlobTable>) -> Self {
        let data = inode_data
        .data
        .iter()
        .enumerate()
        .map(|(idx, chunk)| {
            let chunk_info = inode.get_chunk_info(idx as u32).unwrap();
            Arc::new(DedupChunkInfo {
                blob_index: chunk.blob_index,
                index: chunk.index,
                compress_offset: chunk.compress_offset,
                decompress_offset: chunk.decompress_offset,

                block_id: Arc::new(chunk_info.block_id().clone()),
                compr_size: chunk_info.compress_size(),
                decompress_size: chunk_info.decompress_size(),
            })
        })
        .collect::<Vec<_>>();

        DedupInode {
            data,
            blob_table,
        }
    }

    // fn jump_name(&mut self, name_size: usize, r: &mut RafsIoReader) -> Result<()> {
    //     if name_size > 0 {
    //         r.seek(SeekFrom::Current(name_size as i64))?;
    //     }
    //     r.seek_to_next_aligned(name_size, RAFSV5_ALIGNMENT)?;
    //     Ok(())
    // }

    // fn jump_symlink(&mut self, mode: u32, symlink_size: usize, r: &mut RafsIoReader) -> Result<()> {
    //     if (mode & libc::S_IFMT == libc::S_IFLNK) && symlink_size > 0 {
    //         r.seek(SeekFrom::Current(symlink_size as i64))?;
    //     }
    //     r.seek_to_next_aligned(symlink_size, RAFSV5_ALIGNMENT)?;
    //     Ok(())
    // }

    // fn jump_xattr(&mut self, flags: RafsV5InodeFlags, r: &mut RafsIoReader) -> Result<()> {
    //     if flags.contains(RafsV5InodeFlags::XATTR) {
    //         let mut xattrs = RafsV5XAttrsTable::new();
    //         r.read_exact(xattrs.as_mut())?;
    //         r.seek(SeekFrom::Current(xattrs.aligned_size() as i64))?;
    //     }
    //     Ok(())
    // }

    // fn load_chunk_info(&mut self, mode: u32, child_cnt: u32, r: &mut RafsIoReader) -> Result<()> {
    //     if (mode & libc::S_IFMT == libc::S_IFREG) && child_cnt > 0 {
    //         let mut chunk = RafsV5ChunkInfo::new();
    //         for _i in 0..child_cnt {
    //             chunk.load(r)?;
    //             self.data.push(Arc::new(DedupChunkInfoV5::from(&chunk)));
    //         }
    //     }
    //     Ok(())
    // }

    // pub fn load(&mut self, r: &mut RafsIoReader) -> Result<()> {
    //     // RafsV5Inode...name...symbol link...chunks
    //     let mut inode = RafsV5Inode::new();
    //
    //     // parse ondisk inode
    //     // RafsV5Inode|name|symbol|xattr|chunks
    //     r.read_exact(inode.as_mut())?;
    //     // self.copy_from_ondisk(&inode);
    //     self.ino = inode.i_ino;
    //
    //     // jump name symlink xattr 
    //     self.jump_name(inode.i_name_size as usize, r)?;
    //     self.jump_symlink(inode.i_mode, inode.i_symlink_size as usize, r)?;
    //     self.jump_xattr(inode.i_flags, r)?;
    //     self.load_chunk_info(inode.i_mode, inode.i_child_count, r)?;
    //     Ok(())
    // }

    #[inline]
    pub fn get_chunk_info(&self, idx: u32) -> Result<Arc<dyn RafsChunkInfo>> {
        Ok(self.data[idx as usize].clone())
    }

    #[inline]
    pub fn get_blob_by_index(&self, idx: u32) -> Result<Arc<RafsBlobEntry>> {
        self.blob_table.get(idx)
    }
}

#[derive(Clone, Default, Debug)]
pub struct DedupChunkInfo {
    block_id: Arc<RafsDigest>,
    blob_index: u32,
    index: u32,
    compress_offset: u64,
    decompress_offset: u64,
    compr_size: u32,
    decompress_size: u32,
}

// impl DedupChunkInfo {
//     pub fn new() -> Self {
//         DedupChunkInfo {
//             ..Default::default()
//         }
//     }
//
//     pub fn load(&mut self, r: &mut RafsIoReader) -> Result<()> {
//         let mut chunk = RafsV5ChunkInfo::new();
//
//         r.read_exact(chunk.as_mut())?;
//         self.copy_from_ondisk(&chunk);
//
//         Ok(())
//     }
//
//     fn copy_from_ondisk(&mut self, chunk: &RafsV5ChunkInfo) {
//         self.compress_offset = chunk.compress_offset;
//         self.decompress_offset = chunk.decompress_offset;
//     }
// }

impl RafsChunkInfo for DedupChunkInfo {
    fn block_id(&self) -> &RafsDigest {
        &self.block_id
    }

    fn is_compressed(&self) -> bool {
        unimplemented!()
    }

    fn is_hole(&self) -> bool {
        unimplemented!()
    }

    fn flags(&self) -> RafsChunkFlags {
        unimplemented!()
    }

    fn file_offset(&self) -> u64 {
        unimplemented!()
    }
    impl_getter!(blob_index, blob_index, u32);
    impl_getter!(index, index, u32);
    impl_getter!(compress_offset, compress_offset, u64);
    impl_getter!(compress_size, compr_size, u32);
    impl_getter!(decompress_offset, decompress_offset, u64);
    impl_getter!(decompress_size, decompress_size, u32);
}

// impl From<&RafsV5ChunkInfo> for DedupChunkInfoV5 {
//     fn from(info: &RafsV5ChunkInfo) -> Self {
//         let mut chunk = DedupChunkInfoV5::new();
//         chunk.copy_from_ondisk(info);
//         chunk
//     }
// }


// ------------------
#[derive(Serialize, Deserialize, Debug)]
pub struct InodeData {
    id:   String,
    data: Vec<ChunkData>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChunkData {
    blob_index: u32,
    index: u32,
    compress_offset: u64,
    decompress_offset: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BlobTableData {
    pub entries: Vec<BlobEntryData>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BlobEntryData {
    pub chunk_count: u32,
    pub readahead_offset: u32,
    pub readahead_size: u32,
    pub blob_id: String,
    pub blob_index: u32,
    pub blob_cache_size: u64,
    pub compressed_blob_size: u64,
}
// ------------------



// ------------------
#[derive(Serialize, Deserialize, Debug)]
pub struct Request {
    op: String,
    kind: String,
    key: Option<String>,
    value: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    status: String,
    message: Option<String>,
    data: Option<serde_json::Value>,
}

pub struct DedupClient {
    stream: UnixStream,
}

impl DedupClient {
    pub fn connect(path: &String) -> Result<Self> {
        let stream = UnixStream::connect(path)?;
        Ok(Self { stream })
    }

    fn send_message<T: Serialize>(&mut self, msg: &T) -> Result<()> {
        let data = serde_json::to_vec(msg).unwrap();
        self.stream.write_u32::<BigEndian>(data.len() as u32)?;
        self.stream.write_all(&data)?;
        Ok(())
    }

    fn read_message(&mut self) -> Result<Response> {
        let len = self.stream.read_u32::<BigEndian>()?;
        let mut buf = vec![0; len as usize];
        self.stream.read_exact(&mut buf)?;
        let resp: Response = serde_json::from_slice(&buf)?;
        Ok(resp)
    }

    pub fn put_inode(&mut self, key: &String, value: &InodeData) -> Result<Response> {
        let req = Request {
            op: "PUT".into(),
            kind: "inode".into(),
            key: Some(key.clone()),
            value: Some(serde_json::to_value(value).unwrap()),
        };
        self.send_message(&req)?;
        self.read_message()
    }

    pub fn get_inode(&mut self, key: &String) -> Result<Response> {
        let req = Request {
            op: "GET".into(),
            kind: "inode".into(),
            key: Some(key.clone()),
            value: None,
        };
        self.send_message(&req)?;
        self.read_message()
    }

    pub fn put_blob(&mut self, key: &String, value: &BlobTableData) -> Result<Response> {
        let req = Request {
            op: "PUT".into(),
            kind: "blob".into(),
            key: Some(key.clone()),
            value: Some(serde_json::to_value(value).unwrap()),
        };
        self.send_message(&req)?;
        self.read_message()
    }

    pub fn get_blob(&mut self, key: &String) -> Result<Response> {
        let req = Request {
            op: "GET".into(),
            kind: "blob".into(),
            key: Some(key.clone()),
            value: None,
        };
        self.send_message(&req)?;
        self.read_message()
    }
}