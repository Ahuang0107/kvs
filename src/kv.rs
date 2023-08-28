use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::{fs, io};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::{KvsError, Result};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

///
pub struct KvStore {
    folder: PathBuf,
    writer: BufWriter<File>,
    readers: BTreeMap<u64, BufReader<File>>,
    cur_gen: u64,
    index: BTreeMap<String, (u64, u64, u64)>,
    uncompacted: u64,
}

impl KvStore {
    ///
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        use std::fs::read_dir;
        let folder = path.as_ref();
        create_dir_all(folder)?;
        let mut gen_list: Vec<u64> = read_dir(folder)?
            .flat_map(|file| -> Result<_> { Ok(file?.path()) })
            .filter(|f| f.is_file() && f.extension() == Some("log".as_ref()))
            .flat_map(|path| {
                path.file_name()
                    .and_then(OsStr::to_str)
                    .map(|f| f.trim_end_matches(".log"))
                    .map(str::parse::<u64>)
            })
            .flatten()
            .collect();
        gen_list.sort_unstable();
        let mut readers: BTreeMap<u64, BufReader<File>> = BTreeMap::new();
        let mut index: BTreeMap<String, (u64, u64, u64)> = BTreeMap::new();
        let mut uncompacted = 0;
        for &gen_id in &gen_list {
            let file = folder.join(format!("{gen_id}.log"));
            readers.insert(gen_id, BufReader::new(File::open(file)?));
            let reader = readers.get_mut(&gen_id).unwrap();
            let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
            let mut pos = stream.byte_offset();
            while let Some(cmd) = stream.next() {
                match cmd? {
                    Command::Set(key, _) => {
                        let new_pos = stream.byte_offset();
                        index.insert(key, (gen_id, pos as u64, new_pos as u64));
                    }
                    Command::Remove(key) => {
                        index.remove(&key);
                    }
                }
                pos = stream.byte_offset();
            }
            uncompacted += pos as u64;
        }
        let cur_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = BufWriter::new(
            OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(folder.join(format!("{cur_gen}.log")))?,
        );
        let file = folder.join(format!("{cur_gen}.log"));
        readers.insert(cur_gen, BufReader::new(File::open(file)?));
        Ok(Self {
            folder: folder.to_owned(),
            writer,
            readers,
            cur_gen,
            index,
            uncompacted,
        })
    }
    ///
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set(key.clone(), value);
        let before = self.writer.stream_position()?;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        let after = self.writer.stream_position()?;
        self.index.insert(key, (self.cur_gen, before, after));
        self.uncompacted += after - before;
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }
    ///
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some((gen, start, end)) = self.index.get(&key) {
            let reader = self
                .readers
                .get_mut(gen)
                .expect(&format!("unable to find reader for {gen}.log"));
            reader.seek(SeekFrom::Start(*start))?;
            match serde_json::from_reader(reader.take(end - start))? {
                Command::Set(_, v) => {
                    return Ok(Some(v));
                }
                _ => {}
            }
        }
        Ok(None)
    }
    ///
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::Remove(key.clone());
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            self.index.remove(&key);
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
    ///
    fn compact(&mut self) -> Result<()> {
        let compaction_gen = self.cur_gen + 1;
        let compaction_file = self.folder.join(format!("{compaction_gen}.log"));
        let mut compaction_writer = BufWriter::new(
            OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(&compaction_file)?,
        );
        let mut pos = 0;
        for (gen, start, end) in self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(gen)
                .expect(&format!("unable to find reader for {gen}.log"));
            reader.seek(SeekFrom::Start(*start))?;
            let len = io::copy(&mut reader.take(*end - *start), &mut compaction_writer)?;
            *gen = compaction_gen;
            *start = pos;
            pos += len;
            *end = pos;
        }
        self.uncompacted = 0;
        compaction_writer.flush()?;
        for gen_id in self.readers.keys() {
            fs::remove_file(self.folder.join(format!("{gen_id}.log")))?;
        }
        self.cur_gen += 2;
        self.writer = BufWriter::new(
            OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(self.folder.join(format!("{}.log", self.cur_gen)))?,
        );
        self.readers = BTreeMap::new();
        self.readers.insert(
            compaction_gen,
            BufReader::new(File::open(&compaction_file)?),
        );
        let file = self.folder.join(format!("{}.log", self.cur_gen));
        self.readers
            .insert(self.cur_gen, BufReader::new(File::open(file)?));
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    #[serde(rename = "S")]
    Set(String, String),
    #[serde(rename = "R")]
    Remove(String),
}
