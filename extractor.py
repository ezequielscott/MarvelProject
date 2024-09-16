import os
import requests 
import time 
import hashlib
import pandas as pd
import json
import logging

class MarvelExtractor:
    
    def __init__(self, api_public_key, api_private_key):
        """Class constructor

        Attributes:
            api_public_key: A string with the public key 
            api_private_key: A string with the private key
        """
        
        self.api_public_key = api_public_key
        self.api_private_key = api_private_key
        
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler("logs/debug.log", mode="w"),
                logging.StreamHandler()
            ]
        )
        
        
    def get_params(self):
        """Get a dictionary with the parameters required by the API.
        
        The dictionary includes:
            ts: current timestamp
            apikey: public api key
            hash: a md5 digest made of ts+public_key+private_key
            limit: max number of records to retrieve
            offset: the initial offset is set to 0
        """
    
        ts = time.time()
        hash_md5 = hashlib.md5()
        hash_md5.update(f'{ts}{ self.api_private_key }{ self.api_public_key }'.encode('utf-8'))
        hashed_params = hash_md5.hexdigest()

        params = {
            'ts': ts,
            'apikey': self.api_public_key,
            'hash': hashed_params,
            'limit': 100,
            'offset': 0
        }
        return params 
            
            
    def save_to_file(self, data, filename):
        """Save a list of dictionaries as a JSON file.
        
        This method is usually used after using `get_characters`.
        
        Attributes:
            filename: the filepath where the data is saved.
        """
        
        logging.info('Saving data to file...')
        
        with open(filename, 'w') as f: 
            json.dump(data, f)
            
        logging.info('Done.')
            
    
    def get_records(self, api_url, limit=0):
        """A generic method to retrieve records from the api

        Args:
            api_url (str): _description_
            limit (int, optional): total number of records to retrieve. Defaults to 0 (all available records are extracted).
        """
        params = self.get_params()
            
        final = []      # the final list to return
        nrecords = 0    # record count
        offset = 100    # max offset allowed by the API
            
        while True: 
            time.sleep(2) # wait before each request
            logging.info(f"Collecting records {nrecords}-{nrecords+offset}...")
            response = requests.get(api_url, params=params) 
                    
            if response.status_code == 200: 
                data = response.json() 
                
                if data["code"] == 200: # correct response
                    
                    header = data["data"]
                    total = header["total"]
                    
                    limit = total if limit == 0 else limit
                    
                    if nrecords == 0: # first iteration
                        logging.info(f"Total available records: {total} ")            
                    
                    final.extend(header['results'])  # 'data' contains the batch of results 
                    
                    nrecords += offset
                    
                    logging.info(f"... sucessfully collected {nrecords} out of {total} records.")
                
                    # Check if there are more results to fetch
                    if nrecords >= limit:
                        break
                    else: # nrecords < total: 
                        params['offset'] = nrecords  # Update the offset parameter 
                                
                else:
                    logging.error(f"API Error: {data.code}") 
                    time.sleep(2)   # Simple backoff on error 
                    continue        # Retry the same request 
            else: 
                logging.error(f"Error during request: {response.status_code}, {response.text}") 
                time.sleep(2)  # Simple backoff on error 
                continue       # Retry the same request
            
        return final
            

    def get_characters(self, limit=0): 
        """Get a list of all the Marvel characters. The list contains dictionaries with characters' attributes. 
        See https://developer.marvel.com/docs#!/public/getCreatorCollection_get_0 
        
        The number of records retrieved by call is set to 100 (max records allowed by the API).
        
        Attributes:
            limit: max number of records to retrieve. All the records are retrieved by default.
        """
        
        api_url = "http://gateway.marvel.com/v1/public/characters" 
        final = self.get_records(api_url, limit)
        return final 
    
    
    def preprocess_characters(self, output_filename, data_input=None, file_input=None):
        """Preprocess a list of Marvel characters. 
        As a result, a csv file is created with the following attributes:
        * id: unique id of the character
        * name: name of the character
        * img: url pointing to the thumbnail of the character
        * comics: number of comics the character appears in
        
        Attributes:
            output_filename: filename where the output will be saved
            data_input: an in-memory list of dictionaries containing characters data downloaded with `get_characters`
            file_input: a json file containing characters data
        """        
        logging.info('Loading data...')
        
        df = pd.DataFrame()
        if (file_input is None) and (data_input is None):
            logging.error('You must provide data either in file format or dictorionary.')
            return 
        elif data_input:
            df = pd.DataFrame(data=data_input)
        else:
            df = pd.read_json("data/characters.json")
        
        logging.info('Preprocessing...')
        
        df["img"] = df.apply(lambda x : x['thumbnail']["path"] + "." + x['thumbnail']["extension"], axis=1)
        
        df["comics"] = df.apply(lambda x : x['comics']["available"], axis=1) 
        
        logging.info('Saving preprocessed data to csv...')
        
        df[["id", "name", "img", "comics"]].to_csv(output_filename, index=False)
        
        logging.info('Done.')
        
        
    def get_character_comics(self, character_id, limit=0):
        """Gets all the comics in which a given character appears in

        Args:
            character_id (int): the id of the character
            limit (int, optional): total number of records to retrieve. Defaults to 0 (all available records are extracted).
        """
        
        api_url = f"http://gateway.marvel.com/v1/public/characters/{character_id}/comics" 
        final = self.get_records(api_url, limit)
        return final

        
    def get_comics(self, limit=0):
        """Gets a list of all the available Marvel comics. The result is a list of dictionaries with comic's attributes. 
        More details https://developer.marvel.com/docs#!/public/getCreatorCollection_get_0 
        
        The number of records retrieved by call is set to 100 (max records allowed by the API).
        
        Attributes:
            limit: total number of records to retrieve. All the records are retrieved by default.
        
        """
        
        api_url = "http://gateway.marvel.com/v1/public/comics"
        final = self.get_records(api_url, limit)
        return final
        

if __name__ == '__main__':
    
    private_key = os.environ['API_PRIVATE_KEY']
    public_key = os.environ['API_PUBLIC_KEY']
    
    # init the extractor
    mv = MarvelExtractor(public_key, private_key)
    
    ###
    ### get characters
    ###
    data = mv.get_characters()
    mv.save_to_file(data, "data/characters.json")
    mv.preprocess_characters(data_input=data, output_filename="data/characters.csv")
    

    
    
    