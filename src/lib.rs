use gulkana;
use rand::prelude::*;
use std::collections::HashMap;
#[derive(Clone,PartialEq)]
pub struct DB_Entry{
    path:String,
}
fn db_entry_to_map(db_entry: DB_Entry)->HashMap<String,String>{
    let mut out:HashMap<String,String>=HashMap::new();
    out.insert("path".to_string(),db_entry.path);
    return out;
    
}
fn map_to_db_entry(map:HashMap<String,String>)->Result<DB_Entry,String>{
    let opt = map.get(&"path".to_string());
    if opt.is_some(){
        let path:String = opt.unwrap().clone();
     
        let mut out=DB_Entry{
            path:path,
        };
        return Ok(out);
    }else{
        return Err("path not found in map".to_string());
    }
}
pub struct Database{
    //root file system has key of 0
    db:gulkana::DataStructure<u32,HashMap<String,String>>,
    rng:ThreadRng,
}
type KeyType=u32;
impl Database{
    pub fn insert(&mut self,input: DB_Entry,parent:KeyType)->Result<KeyType,String>{
        let temp_key:KeyType = self.rng.gen();
        println!("key: {}",temp_key);
        if !self.db.contains(&temp_key){
            let res = self.db.insert(&temp_key,db_entry_to_map(input));
            if res.is_ok(){
                let res2 = self.db.appendLinks(&parent,&temp_key);
                if res2.is_ok(){
                    return Ok(temp_key);
                }else{
                    return Err(res2.err().unwrap());
                }
            }else{
                return Err(res.err().unwrap());
            }
        }else{
            println!("does not contain key: {}",temp_key);

            return self.insert(input,parent);
        }
    }
    pub fn get(&self,key:KeyType)->Result<DB_Entry,String>{
        let hash = self.db.get(key);
        if hash.is_some(){
            return map_to_db_entry(hash.unwrap()); 
        }else{
            return Err("key not found".to_string());
        }
    }
    pub fn makeDir(&mut self)->Option<KeyType>{
        let temp_key = self.rng.gen();
        let res = self.db.insertLink(&temp_key,&vec![]);
        if res.is_ok(){
            return Some(temp_key);
        }else{
            return None;
        }
    }
    pub fn getNodeChildren(&self,key:KeyType)->Option<Vec<KeyType>>{
        return self.db.getLinks(&key);

    }
}
pub fn new()->Database{
    let mut db = Database{
        db:gulkana::new_datastructure(),
        rng:rand::thread_rng(),
    };
    db.db.insertLink(&0,&vec![]);
    return db;
}

#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn insert(){
        let in_db = DB_Entry{
            path:"foo".to_string(),
        };
        let mut db = new();
        let key_res = db.insert(in_db.clone(),0);
        let out = db.get(key_res.unwrap());
        assert!(in_db==out.unwrap());
    }
    #[test]
    fn get_root_children(){
        let mut db = new();
        let in_db = DB_Entry{
            path:"foo".to_string(),
        };
        let key = db.insert(in_db.clone(),0).unwrap();
        let dir = db.makeDir().unwrap();
        let key2 = db.insert(in_db,dir).unwrap();
        let v = db.getNodeChildren(0).unwrap(); 
        for i in v{
            assert!(i==key);
        }
        let v2 = db.getNodeChildren(dir).unwrap();
        for i in v2{
            assert!(i==key2);
        }
    
    }
    #[test]
    fn mass_insert(){
        let in_db = DB_Entry{
            path:"foo".to_string(),
        };
        let mut db = new();
        for i in 1..10000{
            let key_res = db.insert(in_db.clone(),0);
        }

    }
}
