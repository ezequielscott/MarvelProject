import unittest
import os
from extractor import MarvelExtractor
import pandas as pd

class TestMv(unittest.TestCase):
    
    def setUp(self):
        private_key = os.environ['API_PRIVATE_KEY']
        public_key = os.environ['API_PUBLIC_KEY']
        
        # init the extractor
        self.mv = MarvelExtractor(public_key, private_key)
        
    def test_ncharacters(self):
        
        limit = 300
        data = self.mv.get_characters(limit=limit)
        df = pd.DataFrame(data=data)  
        
        self.assertEqual(df["id"].nunique(), limit)
        
    def test_nodup(self):
        
        limit = 300
        data = self.mv.get_characters(limit=limit)
        df = pd.DataFrame(data=data)    
        
        self.assertFalse(df.groupby("name")["id"].nunique().any() > 1)    
     
     
    def test_transform(self):
        
        limit = 300
        data = self.mv.get_characters(limit=limit)
        df = pd.DataFrame(data=data)  
        
        self.assertEqual(df["id"].nunique(), limit)
        
        self.mv.preprocess_characters(output_filename='data/test.csv', data_input=data)
        
        df2 = pd.read_csv('data/test.csv')
        
        self.assertEqual(df2["id"].nunique(), limit)
         
        
    def test_num_comics(self):
        
        test_data = [
            { 
                'id' : 1010699,
                'name' : 'Aaron Stack',
                'count' : 14
            },
            { 
                'id' : 1011096,
                'name' : 'Captain Marvel (Genis-Vell)',
                'count' : 70
            },
            { 
                'id' : 1009257,
                'name' : 'Cyclops',
                'count' : 997
            },
            { 
                'id' : 1009262,
                'name' : 'Daredevil',
                'count' : 1283
            }                  
        ] 
        
        df = pd.read_json('data/characters.json')
        for t in test_data:
            self.assertTrue( df.loc[ df["id"] == t["id"], "comics" ].values[0], t["count"] )
            

if __name__ == '__main__':
    unittest.main()