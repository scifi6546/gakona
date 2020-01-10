use gulkana;
use rand::prelude::*;
use std::collections::HashMap;
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
}
impl FSError{
    fn to_string(self)->String{
        match self{
            KeyAllreadyPresent=>"KeyAllreadyPresent".to_string(),
            KeyNotFound=>"KeyNotFound".to_string(),
            NodeNotLink=>"NodeNotLink".to_string(),
            NodeNotData=>"NodeNotData".to_string(),
            PathNotPresent=>"PathNotPresent".to_string(),
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
            return Ok(temp_key);
        }else{
            return Err(FSError::KeyNotFound);
        }
    }
    pub fn get(&self,key:&KeyType)->Result<DBExtType,FSError>{
        let hash = self.db.get(key)?;
        return map_to_db_entry(hash); 
    }
    pub fn make_dir(&mut self)->Option<KeyType>{
        let temp_key = self.rng.gen();
        let res = self.db.insert_link(&temp_key,&vec![]);
        if res.is_ok(){
            return Some(temp_key);
        }else{
            return None;
        }
    }
    fn make_dir_key(&mut self,key:&KeyType)->Result<(),FSError>{
        let res = self.get(key);
        //if key is not present
        if res.is_err(){
            self.db.insert_link(key,&vec![])?;
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
}
pub fn new()->Database{
    let mut db = Database{
        db:gulkana::new_datastructure(),
        rng:rand::thread_rng(),
    };
    db.make_dir_key(&0);
    return db;
}

#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn insert(){
        let in_db = DBEntry{
            path:"foo".to_string(),
        };
        let mut db = new();
        let key_res = db.insert(in_db.clone(),0);
        if key_res.is_ok(){
            let out = db.get(&key_res.ok().unwrap());
            assert!(in_db==out.ok().unwrap());
        }else{
            println!("{}",key_res.err().unwrap().to_string());
            assert!(1==0);
        }
    }
    #[test]
    fn get_root_children(){
        let mut db = new();
        let in_db = DBEntry{
            path:"foo".to_string(),
        };
        let key = db.insert(in_db.clone(),0).ok().unwrap();
        let dir = db.make_dir().unwrap();
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
        let mut db = new();
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
        let mut db = new();
        for i in 1..10000{
            let key_res = db.insert(in_db.clone(),0);
        }

    }
}
