use gulkana;
use rand::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write; 
use std::rc::Rc;
use std::path::Path;

#[derive(Clone,PartialEq)]
pub struct DBEntry{
    path:String,
}
pub enum FSError{
    KeyAllreadyPresent,
    KeyNotFound,
    NodeNotLink,
    NodeNotData,
    PathNotPresent,
    FileError,
    SerializeError,
}
impl FSError{
    #[allow(dead_code)]
    fn to_string(self)->String{
        match self{
            FSError::KeyAllreadyPresent=>"KeyAllreadyPresent".to_string(),
            FSError::KeyNotFound=>"KeyNotFound".to_string(),
            FSError::NodeNotLink=>"NodeNotLink".to_string(),
            FSError::NodeNotData=>"NodeNotData".to_string(),
            FSError::PathNotPresent=>"PathNotPresent".to_string(),
            FSError::FileError=>"File Error".to_string(),
            FSError::SerializeError=>"Error when Serializing to String".to_string(),
        }
    }
}
type DBType=HashMap<String,String>;
pub type DBExtType = DBEntry;
impl From<gulkana::DBOperationError> for FSError{
    fn from(error:gulkana::DBOperationError)->Self{
        match error{
            gulkana::DBOperationError::KeyAllreadyPresent
                =>FSError::KeyAllreadyPresent,
            gulkana::DBOperationError::KeyNotFound
                =>FSError::KeyNotFound,
            gulkana::DBOperationError::NodeNotLink
                =>FSError::NodeNotLink,
            gulkana::DBOperationError::NodeNotData
                =>FSError::NodeNotData

        }
    }
}
impl From<std::io::Error> for FSError{

    fn from(error: std::io::Error)->Self{
        match error{
            _ =>FSError::FileError,
        }
    }
}

impl From<gulkana::SerializeError> for FSError{

    fn from(error: gulkana::SerializeError)->Self{
        match error{
            _ =>FSError::SerializeError,
        }
    }
}

fn db_entry_to_map(db_entry: &DBEntry)->HashMap<String,String>{
    let mut out:HashMap<String,String>=HashMap::new();
    out.insert("path".to_string(),db_entry.path.clone());
    return out;
    
}
fn map_to_db_entry(map:&HashMap<String,String>)->Result<DBEntry,FSError>{
    let opt = map.get(&"path".to_string());
    if opt.is_some(){
        let path:String = opt.unwrap().clone();
     
        Ok(DBEntry{
            path:path,
        })
    }else{
        return Err(FSError::PathNotPresent);
    }
}
pub struct Database{
    //root file system has key of 0
    db:gulkana::DataStructure<u32,DBType>,
    rng:ThreadRng,
    file_backing:Option<std::string::String>
}
pub struct ChildIter<'a>{
    db:&'a Database,
    iter:std::slice::Iter<'a,KeyType>
}
impl<'a> Iterator for ChildIter<'a>{
    type Item = (& 'a KeyType,DBExtType);
    fn next(&mut self)->Option<Self::Item>{
        let index = self.iter.next()?;
        let res = self.db.get(index);
        if res.is_ok(){
            return Some((index,res.ok().unwrap()));
        }else{
            return None;
        }
    }
}
type KeyType=u32;
impl Database{
    pub fn insert(&mut self,input: DBEntry,parent:KeyType)->Result<KeyType,FSError>{
        let temp_key:KeyType = self.rng.gen();
        //checking if parent exists
        if self.db.contains(&parent){
            self.db.insert(&temp_key,db_entry_to_map(&input))?;
            self.db.append_links(&parent,&temp_key)?;
            self.write_file()?;
            return Ok(temp_key);
        }else{
            return Err(FSError::KeyNotFound);
        }
    }
    pub fn get(&self,key:&KeyType)->Result<DBExtType,FSError>{
        let hash = self.db.get(key)?;
        return map_to_db_entry(hash); 
    }
    pub fn make_dir(&mut self)->Result<KeyType,FSError>{
        let temp_key = self.rng.gen();
        self.db.insert_link(&temp_key,&vec![])?;
        self.write_file()?;
        return Ok(temp_key);

    }
    fn make_dir_key(&mut self,key:&KeyType)->Result<(),FSError>{
        let res = self.get(key);
        //if key is not present
        if res.is_err(){
            self.db.insert_link(key,&vec![])?;
            self.write_file()?;
            return Ok(());
        }else{

            return Err(FSError::KeyAllreadyPresent);
        }
    }
    pub fn get_node_children(&self,key:KeyType)->Result<&Vec<KeyType>,FSError>{
        let link = self.db.get_links(&key)?;
        return Ok(link);

    }
    pub fn iter_children(&self,key:&KeyType)->Result<ChildIter,FSError>{
        Ok(ChildIter{
            db:self,
            iter:self.db.get_links(key)?.iter()
        })
    }
    fn write_file(&self)->Result<(),FSError>{
        if self.file_backing.is_some(){
            let p = Path::new(self.file_backing.as_ref().unwrap().as_str());
            let mut file = File::create(p)?;
            file.write(self.db.to_string()?.as_bytes().as_ref())?;
            return Ok(());
        }else{
            return Ok(());
        }
    }
}
pub fn new()->Result<Database,FSError>{
    let mut db = Database{
        db:gulkana::new_datastructure(),
        rng:rand::thread_rng(),
        file_backing:None,
    };
    
    db.make_dir_key(&0)?;
    return Ok(db);
}
pub fn new_backed<'a>(path:std::string::String)->Result<Database,FSError>{
    let mut db = new()?;
    db.file_backing=Some(path);
    db.write_file()?;
    return Ok(db);
}

#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    #[allow(unused_must_use)]
    fn insert(){
        let in_db = DBEntry{
            path:"foo".to_string(),
        };
        let mut db = new().ok().unwrap();
        let key_res = db.insert(in_db.clone(),0).ok().unwrap();
        let out = db.get(&key_res);
        assert!(in_db==out.ok().unwrap());
    }
    #[test]
    fn get_root_children(){
        let mut db = new().ok().unwrap();
        let in_db = DBEntry{
            path:"foo".to_string(),
        };
        let key = db.insert(in_db.clone(),0).ok().unwrap();
        let dir = db.make_dir().ok().unwrap();
        let key2 = db.insert(in_db,dir).ok().unwrap();
        let v = db.get_node_children(0).ok().unwrap(); 
        for i in v{
            assert!(i==&key);
        }
        let v2 = db.get_node_children(dir).ok().unwrap();
        for i in v2{
            assert!(i==&key2);
        }
    
    }
    #[test]
    fn iterate_root(){
        let mut db = new().ok().unwrap();
        let key = db.insert(DBExtType{path:"foo".to_string()},0).ok().unwrap();
        for (key_t,data) in db.iter_children(&0).ok().unwrap(){
            assert!(key_t==&key&&data.path=="foo".to_string());
        }
        
    }
    #[test]
    fn mass_insert(){
        let in_db = DBEntry{
            path:"foo".to_string(),
        };
        let mut db = new().ok().unwrap();
        for _i in 1..10000{
            let key_res = db.insert(in_db.clone(),0);
            assert!(key_res.is_ok());
        }

    }
}
