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
    pub fn insert(&mut self,input: DB_Entry)->Result<KeyType,String>{
        let temp_key:KeyType = self.rng.gen();
        if self.db.contains(&temp_key){
            let res = self.db.insert(&temp_key,db_entry_to_map(input));
            if res.is_ok(){
                let res2 = self.db.appendLinks(&0,&temp_key);
                if res2.is_ok(){
                    return Ok(temp_key);
                }else{
                    return Err(res2.err().unwrap());
                }
            }else{
                return Err(res.err().unwrap());
            }
        }else{
            return self.insert(input);
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
        let key_res = db.insert(in_db.clone());
        let out = db.get(key_res.unwrap());
        assert!(in_db==out.unwrap());
    }
}
