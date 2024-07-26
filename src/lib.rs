use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

/// Any struct that wants to be managed by a collection
/// needs to satisfy these traits
pub trait Document<T> {
    fn uuid(&self) -> Uuid;
    fn does_not_clash(&self, doc: &T) -> Result<(), &str>;
}

/// A collection manages a set of Documents
/// that we want to persist beyond the life
/// of the service.
pub struct Collection<T> {
    _p: PhantomData<T>,
    uuid_to_idx: HashMap<Uuid, usize>,
    max_byte_length: usize,
    byte_length_increment: usize,
    file: File,
    fp: PathBuf,
    count: usize,
}

impl<T> Collection<T>
where
    T: Document<T> + DeserializeOwned + Serialize + Debug,
{
    /// Create a new collection.
    /// Accepts an options PathBuf for writing to the filesystem.
    /// An In-Memory DB.
    /// bli -byte lenght
    pub fn new(fp: PathBuf, bli: Option<usize>) -> Result<Self, String> {
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(fp.clone());
        match file {
            Ok(file) => {
                let mut collection = Collection {
                    _p: PhantomData,
                    uuid_to_idx: HashMap::new(),
                    max_byte_length: 64,
                    byte_length_increment: bli.unwrap_or(64),
                    file,
                    fp,
                    count: 0,
                };
                collection.load_indexes();
                return Ok(collection);
            }
            Err(msg) => return Err(msg.to_string()),
        }
    }

    pub fn new_arc(fp: PathBuf, bli: Option<usize>) -> Result<Arc<RwLock<Collection<T>>>, String> {
        let collection = Collection::new(fp, bli);
        match collection {
            Ok(c) => {
                return Ok(Arc::new(RwLock::new(c)));
            }
            Err(msg) => return Err(msg.to_string()),
        }
    }

    pub fn load_indexes(&mut self) {
        println!("{} > File Path Provided", Utc::now());
        let file = &self.file;
        let reader = BufReader::new(file);
        for (idx, line) in reader.lines().enumerate() {
            let line = line.unwrap();
            let document = serde_json::from_str::<T>(&line.trim());
            if document.is_err() {
                break;
            }
            let document = document.unwrap();
            let key = document.uuid();
            self.uuid_to_idx.insert(key, idx);
            self.count = idx + 1
        }
    }

    pub fn insert(&mut self, doc: T) -> Result<(), &str> {
        let key = doc.uuid();
        if self.uuid_to_idx.contains_key(&key) {
            return Err("Primary key used");
        }

        let reader = BufReader::new(&self.file);
        for line in reader.lines() {
            let line = line.unwrap();
            // existing document
            let edoc = serde_json::from_str::<T>(&line.trim());
            if edoc.is_err() {
                break;
            }
            let edoc = edoc.unwrap();
            let ans = edoc.does_not_clash(&doc);
            match ans {
                Ok(()) => {}
                Err(_) => return Err("Clash occurred"),
            }
        }

        let string = serde_json::to_string(&doc);
        if string.is_err() {
            return Err("Error turning struct into JSON");
        }
        let string = string.unwrap();
        let byte_length = string.len();
        if byte_length > self.max_byte_length {
            let div = (byte_length / self.byte_length_increment) + 1;
            self.max_byte_length = self.byte_length_increment * div;
            println!(
                "{} > DB Resize New Byte Length: {}",
                Utc::now(),
                self.max_byte_length
            );
            let resize_success = self.resize_db();
            if resize_success.is_err() {
                return Err("Failed to resize DB");
            }
        }
        let padded_string = format!("{:width$}\n", string, width = self.max_byte_length);
        let offset: u64 = (self.count * (self.max_byte_length + 1))
            .try_into()
            .unwrap();
        let write_success = self.file.write_at(padded_string.as_bytes(), offset);
        if write_success.is_err() {
            return Err("Failed to write");
        }
        //file.flush().unwrap();
        self.uuid_to_idx.insert(doc.uuid(), self.count);
        self.count += 1;

        return Ok(());
    }

    fn resize_db(&mut self) -> Result<(), &str> {
        fs::copy(&self.fp, "tmp.col").unwrap();

        let mut tmp_path = std::env::current_dir().unwrap();
        tmp_path.push("tmp.col");
        let tmp_file = fs::OpenOptions::new().read(true).open(tmp_path);
        if tmp_file.is_err() {
            return Err("Error opening tmp file for db resize");
        }
        let tmp_file = tmp_file.unwrap();
        let tmp_reader = BufReader::new(tmp_file);

        let cleared = self.file.set_len(0);
        if cleared.is_err() {
            return Err("Failed to clear contents of DB.");
        }
        for (idx, line) in tmp_reader.lines().enumerate() {
            if line.is_err() {
                println!("Hello");
                return Err("Line error");
            }
            let line = line.unwrap();
            let repadded_string = format!("{:width$}\n", line, width = self.max_byte_length);
            let offset: u64 = (idx * (self.max_byte_length + 1)).try_into().unwrap();
            let write_success = self.file.write_at(repadded_string.as_bytes(), offset);
            if write_success.is_err() {
                return Err("Failed to write");
            }
        }
        fs::remove_file("tmp.col").unwrap();
        Ok(())
    }

    /// Update a document
    pub fn update(&mut self, doc: T) -> Result<(), &str> {
        // Make sure we're at the start
        let mut reader = BufReader::new(&self.file);
        reader.seek(std::io::SeekFrom::Start(0)).unwrap();
        for line in reader.lines() {
            let line = line.unwrap();
            // existing document
            let edoc = serde_json::from_str::<T>(&line.trim());
            if edoc.is_err() {
                break;
            }
            let edoc = edoc.unwrap();
            if edoc.uuid() != doc.uuid() {
                let ans = edoc.does_not_clash(&doc);
                match ans {
                    Ok(()) => {}
                    Err(_) => return Err("Clash occurred"),
                }
            }
        }

        // Update DB.
        let string = serde_json::to_string(&doc);
        if string.is_err() {
            return Err("Error turning struct into JSON");
        }
        let string = string.unwrap();
        let byte_length = string.len();
        if byte_length > self.max_byte_length {
            let div = (byte_length / self.byte_length_increment) + 1;
            self.max_byte_length = self.byte_length_increment * div;
            let resize_success = self.resize_db();
            if resize_success.is_err() {
                return Err("Failed to resize DB");
            }
        }

        let idx = self.uuid_to_idx.get(&doc.uuid());
        if idx.is_none() {
            return Err("No idx found");
        }
        let idx = idx.unwrap();

        let padded_string = format!("{:width$}\n", string, width = self.max_byte_length);
        // Write right location in the file
        let offset: u64 = (idx * (self.max_byte_length + 1)).try_into().unwrap();
        let write_success = self.file.write_at(padded_string.as_bytes(), offset);
        if write_success.is_err() {
            return Err("Failed to write");
        }

        return Ok(());
    }

    /// Find all documents that meet the criteria.
    /// Returns a vector of immutable references.
    pub fn filter(&self, filter_fcn: impl Fn(&T) -> bool) -> Vec<T> {
        let mut docs: Vec<T> = vec![];
        let mut reader = BufReader::new(&self.file);
        reader.seek(std::io::SeekFrom::Start(0)).unwrap();
        for line in reader.lines() {
            let line = line.unwrap();
            // existing document
            let edoc = serde_json::from_str::<T>(&line.trim());
            if edoc.is_err() {
                break;
            }
            let edoc = edoc.unwrap();
            if filter_fcn(&edoc) {
                docs.push(edoc);
            }
        }
        return docs;
    }

    /// Find the first document that satisfies the criteria.
    pub fn find(&self, find_fcn: impl Fn(&T) -> bool) -> Option<T> {
        let mut reader = BufReader::new(&self.file);
        reader.seek(std::io::SeekFrom::Start(0)).unwrap();
        for line in reader.lines() {
            let line = line.unwrap();
            // existing document
            let edoc = serde_json::from_str::<T>(&line.trim());
            if edoc.is_err() {
                break;
            }
            let edoc = edoc.unwrap();
            if find_fcn(&edoc) {
                return Some(edoc);
            }
        }
        return None;
    }

    /// Get a document by its uuid
    pub fn by_uuid(&self, uuid: &Uuid) -> Option<T> {
        let idx = self.uuid_to_idx.get(uuid);
        if idx.is_none() {
            return None;
        }
        let idx = idx.unwrap();
        let mut reader = BufReader::new(&self.file);
        let offset: u64 = (idx * (self.max_byte_length + 1)).try_into().unwrap();
        let pos = SeekFrom::Start(offset);
        reader.seek(pos).unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let edoc = serde_json::from_str::<T>(&line.trim());
        if edoc.is_err() {
            return None;
        }
        return Some(edoc.unwrap());
    }

    /// Remove a document from the DB
    pub fn delete(&mut self, uuid: &Uuid) -> Result<(), &str> {
        let idx = self.uuid_to_idx.get(uuid);
        if idx.is_none() {
            return Err("No idx found");
        }
        let idx = idx.unwrap().clone();

        // decrement all the indexes above the one being removed
        for (_k, v) in self.uuid_to_idx.iter_mut() {
            if *v > idx {
                *v -= 1;
            }
        }

        // Remove from the map and vec.
        self.uuid_to_idx.remove(uuid);

        // Remove from the file
        fs::copy(&self.fp, "tmp.col").unwrap();
        let mut tmp_path = std::env::current_dir().unwrap();
        tmp_path.push("tmp.col");
        let tmp_file = fs::OpenOptions::new().read(true).open(tmp_path);
        if tmp_file.is_err() {
            return Err("Error opening tmp file for db resize");
        }
        let tmp_file = tmp_file.unwrap();
        let tmp_reader = BufReader::new(tmp_file);

        let cleared = self.file.set_len(0);
        if cleared.is_err() {
            return Err("Failed to clear contents of DB.");
        }
        let mut writer = BufWriter::new(&self.file);
        let pos = SeekFrom::Start(0);
        writer.seek(pos).unwrap();
        for (lidx, line) in tmp_reader.lines().enumerate() {
            if line.is_err() {
                return Err("Line error");
            }
            if idx == lidx {
                continue;
            }
            let line = line.unwrap();
            let repadded_string = format!("{:width$}\n", line, width = self.max_byte_length);
            let write_success = writer.write(repadded_string.as_bytes());
            if write_success.is_err() {
                return Err("Failed to write");
            }
            writer.flush().unwrap();
        }
        fs::remove_file("tmp.col").unwrap();
        return Ok(());
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde::Deserialize;
    use uuid::Uuid;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct User {
        uuid: Uuid,
        name: String,
    }

    impl Document<User> for User {
        fn uuid(&self) -> Uuid {
            return self.uuid;
        }

        fn does_not_clash(&self, doc: &User) -> Result<(), &str> {
            if self.name == doc.name {
                return Err("Email is already in use.");
            }
            return Ok(());
        }
    }

    impl User {
        pub fn new(name: String) -> Self {
            User {
                uuid: Uuid::new_v4(),
                name,
            }
        }
    }

    #[test]
    fn test_insert() {
        let mut fp = std::env::current_dir().unwrap();
        fp.push("user.col");
        let _ = fs::remove_file(fp.clone());
        let mut c = Collection::<User>::new(fp, None).unwrap();

        let user_bob = User::new("bob".to_string());
        let mut user_bob_cloned = user_bob.clone();
        let res: Result<(), &str> = c.insert(user_bob);
        if res.is_err() {
            println!("{:?}", res.unwrap())
        }
        assert_eq!(res.is_ok(), true);

        let user_resize_db_long_name = User::new("user_resize_db_long_name".to_string());
        let res: Result<(), &str> = c.insert(user_resize_db_long_name);
        if res.is_err() {
            println!("{:?}", res.unwrap())
        }
        assert_eq!(res.is_ok(), true);

        user_bob_cloned.name = "Trevor".to_string();
        let res = c.update(user_bob_cloned);
        if res.is_err() {
            println!("{:?}", res.unwrap())
        }
        assert_eq!(res.is_ok(), true);

        let user_bill = User::new("bill".to_string());
        let uuid_bill = user_bill.uuid.clone();
        let res = c.insert(user_bill);
        assert_eq!(res.is_ok(), true);

        let user = User::new("dan".to_string());
        let uuid = user.uuid.clone();
        let res = c.insert(user);
        assert_eq!(res.is_ok(), true);
        let get_user = c.by_uuid(&uuid);
        if get_user.is_some() {
            println!("{:?}", get_user.unwrap());
        }

        let del = c.delete(&uuid_bill);
        assert_eq!(del.is_ok(), true);

        let get_user = c.by_uuid(&uuid);
        if get_user.is_some() {
            println!("{:?}", get_user.unwrap());
        }
    }
}
