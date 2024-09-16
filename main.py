# Retrieving data from RESTful APIs that return results in batches can be optimized using several best practices. Here are some key strategies to consider:

# 1. Understand the API Documentation

# Pagination: Familiarize yourself with how the API handles pagination (e.g., page, limit, offset, etc.). This information is usually provided in the API documentation.
# Rate Limits: Be aware of any rate limits imposed by the API to avoid being throttled or blocked.
# 2. Use Efficient Pagination Techniques

# Cursor-Based Pagination: If available, use cursor-based pagination instead of offset-based pagination. It’s generally more efficient and provides better performance for large datasets.
# Incremental Fetching: Fetch data in increments rather than retrieving all at once. This reduces memory usage and allows for quicker responses.
# 3. Implement Exponential Backoff for Retries

# If you encounter errors (e.g., rate limiting or server errors), use an exponential backoff strategy for retries. This means waiting progressively longer between each retry attempt.
# 4. Batch Processing

# Process Data in Chunks: Instead of waiting for all data to be retrieved, process each batch as it arrives. This can improve responsiveness and reduce memory usage.
# Asynchronous Requests: If the API supports it, consider making asynchronous requests to retrieve multiple batches concurrently.
# 5. Data Caching

# Implement caching mechanisms to store previously fetched results. This can reduce the number of requests made to the API and improve performance.
# 6. Use Filters and Fields

# If the API allows filtering or selecting specific fields, use these features to reduce the amount of data transferred and processed. This can lead to faster responses and lower resource usage.
# 7. Monitor and Log Requests

# Maintain logs of API requests, responses, and errors. This can help you identify patterns, troubleshoot issues, and optimize your data retrieval strategy.
# 8. Error Handling

# Implement robust error handling to manage different types of responses (e.g., success, client errors, server errors). Ensure your application can gracefully handle unexpected situations.
# 9. Timeouts and Cancellation

# Set reasonable timeouts for requests to prevent the application from hanging indefinitely. Implement cancellation mechanisms for long-running requests.
# 10. Throttling Requests

# If the API has strict rate limits, implement throttling in your application to prevent exceeding those limits. This can involve pausing requests when nearing the limit.
# Example Code Snippet

# Here’s a simple example in Python using the requests library to fetch data from a paginated API:

import os
import requests 
import time 
import hashlib
import pandas as pd
import json
import logging

class MarvelExtractor:
    
    def __init__(self, api_public_key, api_private_key):
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
        
        # ts - a timestamp (or other long string which can change on a request-by-request basis)
        # hash - a md5 digest of the ts parameter, your private key and your public key (e.g. md5(ts+privateKey+publicKey)
    
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
        
    def get_comics(self):
        
        api_url = "http://gateway.marvel.com/v1/public/comics"
        params = self.get_params()
                
        response = requests.get(api_url, params=params) 
        
        if response.status_code == 200: 
            data = response.json()
                            
            header = data["data"]
            total = header["total"]
                    
            logging.info(f"Total available records: {total} ")  
            

    def get_characters(self, api_url, params=None): 
        
        api_url = "http://gateway.marvel.com/v1/public/characters" 
        
        if params is None: 
            params = {}
            
        final = [] 
        nrecords = 0
        offset = 100
            
        #final = pd.DataFrame()
        while True: 
            time.sleep(2) # wait before each request
            logging.info(f"Collecting records {nrecords}-{nrecords+offset}...")
            response = requests.get(api_url, params=params) 
                    
            if response.status_code == 200: 
                data = response.json() 
                
                if data["code"] == 200: # correct response
                    
                    header = data["data"]
                    total = header["total"]
                    
                    if nrecords == 0: # first iteration
                        logging.info(f"Total available records: {total} ")            
                    
                    final.extend(header['results'])  # 'data' contains the batch of results 
                    
                    nrecords += offset
                
                    # Check if there are more results to fetch
                    if nrecords < total: 
                        params['offset'] = nrecords  # Update the offset parameter 
                    else: 
                        break  # No more pages 
                                    
                    logging.info(f"... sucessfully collected {nrecords} out of {total} records.")
                    
                    # if nrecords == 300:
                    #     break 
            else: 
                logging.error(f"Error: {response.status_code}, {response.text}") 
                time.sleep(2)  # Simple backoff on error 
                continue  # Retry the same request 
            
            #break # only 1 iterartion
        
        return final 
    
# Example usage 
#data = fetch_data(api_url) 

if __name__ == '__main__':
    
    private_key = os.environ['API_PRIVATE_KEY']
    public_key = os.environ['API_PUBLIC_KEY']
    
    # in case of processing the ts
    # import datetime
    # datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    
    mv = MarvelExtractor(public_key, private_key)
    
    #data = fetch_data(api_url, { "apikey" : os.environ['API_PUBLIC_KEY'], "ts":ts ,"hash" : hashed_params, "offset": 20 } ) 
    #data = mv.fetch_data(api_url, params) 
    
    mv.get_comics()
    
    # logging.info('Saving data to file...')
    
    # with open("data/characters.json", 'w') as f: 
    #     json.dump(data, f)
    
    # #pd.json_normalize(data).to_csv("data/characters.csv", sep=",", encoding="utf-8", index=False)
    
    # logging.info('Reading data file...')

    # df = pd.read_json("data/characters.json")
    
    # print("Unique IDs:" , df["id"].nunique())
    # print("Beasts: ", df[df['name'] == "Beast"]["id"].nunique() )
    
    # thumbnail = df.loc[ df['name'] == "Beast", "thumbnail"].values[0]
    
    # logging.info('Preprocessing...')
    
    # df["img"] = df.apply(lambda x : x['thumbnail']["path"] + "." + x['thumbnail']["extension"], axis=1)
    
    # df["comics"] = df.apply(lambda x : x['comics']["available"], axis=1) 
    
    # logging.info('Saving preprocessed file...')
    
    # df[["id", "name", "img", "comics"]].to_csv("data/characters.csv", index=False)
        
    #available_comics = beast["comics"]
    
    #print(available_comics["available"])
    