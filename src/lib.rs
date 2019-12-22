use gulkana;
use std::collections::HashMap;
struct DB_Entry{
    path:String,
}
pub struct Database{
    //root file system has key of 0
    db:gulkana::DataStructure<u32,HashMap<String,String>>,
}
impl Database{
    pub fn insert(DB_Entry)->u32{
        
    }
}
pub fn new()->Database{
    let mut db = Database{
        db:gulkana::new_datastructure(),
    };
    db.db.insertLink(&0,&vec![]);
    return db;
}

#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn foo(){

        assert!(0==0);
    }
}
