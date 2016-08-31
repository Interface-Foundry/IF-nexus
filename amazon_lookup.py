
import bottlenose, json, time, re
from urllib.error import HTTPError
from pprint import pprint
from bs4 import BeautifulSoup
import pandas as pd
import os


def error_handler(err):
    """
    @param err: error

    This function handles errors recieved from the Amazon Product API
    """
    ex = err['exception']
    if isinstance(ex, HTTPError) and ex.code == 503:
        time.sleep(random.expovariate(0.1))
        return True

AMAZON_ACCESS_KEY = "AKIAIKMXJTAV2ORZMWMQ"
AMAZON_SECRET_KEY = "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"
AMAZON_ASSOC_TAG = "quic0b-20"

amazon = bottlenose.Amazon(AMAZON_ACCESS_KEY, 
						   AMAZON_SECRET_KEY, 
						   AMAZON_ASSOC_TAG, 
						   Parser=BeautifulSoup,
						   MaxQPS=1.0,
						   ErrorHandler=error_handler)

def main():
    """
    Reads in asins from the asins.txt (one asin per line) and sends queries
    the amazon product API. From the response, the asin, title, and product
    group are recorded and written to a data file called 'test.txt'. The time
    that the item was pulled is also recorded. The timestamp is useful since
    items might become depricated and deleted from the database later.
    """
	try:
		os.remove('test.txt')
	except OSError:
		pass

	with open('test.txt', 'w') as f:
		with open('asins.txt', 'r') as f1:
			i = 0
			while i < 100000:
				# each line contains an ASIN.
				# this will bulk read from the file and search.
				line = ','.join([f1.readline().strip() for i in range(10)])
				print(i)
				i += 1
				#line = f1.readline()

				#response = amazon.ItemLookup(ItemId=line.strip(), ResponseGroup="ItemAttributes, Accessories, BrowseNodes")
				response = amazon.ItemLookup(ItemId=line, ResponseGroup="ItemAttributes, Similarities")
				#titles = response.find_all('title')
				#asins = response.find_all('asin')
				#groups = response.find_all('productgroup')
				for item in response.find_all('item'):
					similarproducts = item.find_all('similarproducts')
					
					f.write(item.find('asin').text + '|')
					f.write(item.find('title').text + '|')
					f.write(item.find('productgroup').text + '|')
					f.write(time.strftime("%Y-%m-%d %H:%M") + '|')
					#print("ASIN: %s , Title: %s , Product Group: %s" % (item.find('asin').text, item.find('title').text, item.find('productgroup').text))
					if similarproducts:
					#	print("Similar items: ")
						for similarproduct in similarproducts:
							
							similar_titles = [x.text for x in similarproduct.find_all('title')]
							similar_asins = [x.text for x in similarproduct.find_all('asin')]
							f.write('~~~'.join(similar_titles) + '|')
							f.write('~~~'.join(similar_asins) + '\n')
							#similarities = list(zip(similar_titles, similar_asins))
							
							#f.write('~~~'.join(similarities) + '\n')
					#		print("\tTitle: %s" % ', '.join([x.text for x in similarproduct.find_all('title')]))
					#		print("\tASIN: %s" % ', ' .join([x.text for x in similarproduct.find_all('asin')]))
							#print(similarproduct.prettify())
					else:
						f.write('|\n')
					#print()
				

				#for index, title in enumerate(titles):
				#	f.write(title.text + '|' + asins[index].text + '|' + groups[index].text + '|' + time.strftime("%Y-%m-%d %H:%M") + '\n')
				
				#categories = [tree.find_all('name') for tree in response.find_all('browsenodes')]

				if not line: break

if __name__ == '__main__':
	main()




