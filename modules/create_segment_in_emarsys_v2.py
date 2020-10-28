'''
This program creates customer segment programatically in Emarsys Email Marketing platform.
Dependencies: Adyen transaction files.
Valid Emarsys API access required for this program.
''''

import bootconfig as config
import logging
from adyentransactions import *
from emarsys_api_py_v3 import *  
from argparse import ArgumentParser

df_packages = pd.DataFrame()
df_payments = pd.DataFrame()
processDate = ''

def main():
	global df_packages
	df_packages = config.df_packages
	
	parser = ArgumentParser()
	group = parser.add_mutually_exclusive_group()  # -f and -d options are mutually exclusive
	group.add_argument('-f', action="store", dest="fileName")
	group.add_argument('-d', action="store", dest="processDate", default=(date.today() - timedelta(1)).strftime('%Y_%m_%d')) 
	args = parser.parse_args()

	# input_file = args.fileName
	global processDate
	processDate = args.processDate  
	logFile = ''.join([config.log_dir,os.path.basename(__file__).split('.')[0],'-',processDate,'.log'])
	# print(logFile)
	logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.DEBUG, filename=logFile)
	logging.info("Emarsys segment creation started for the date %s.", processDate)
	df_list = []
	for merchantName in config.adyenMerchants:
		csvFile = 'received_payments_report_'+ processDate +'.csv'
		targetLocation = config.data_dir + merchantName + '/'
		retCode = getAdyenReport(csvFile, targetLocation, merchantName)
		if retCode: logging.info("Adyen transaction file downloaded for %s as %s.", merchantName, retCode)
		input_file = retCode if retCode else None
		if input_file:
			df = getPurchasesAll(input_file, config.packageFile)
			df_list.append(df)
	global df_payments
	df_payments = pd.concat(df_list, axis=0, ignore_index=True)
	config.df_payments = df_payments
	logging.info('\n %s' ,  df_payments.groupby(by='PackageName').size().sort_values(ascending=False))
	
	for key, value in config.emarsysConfig["segmentPatterns"].items():
		# package_pattern = value
		# df_payments_package = getPurchases(df_payments, package_pattern)
		logging.info("Emarsys segment creation started for the %s subscription.", key)
		df_payments_package = getPurchases(df_payments, key)

		segmentName = "Segment - {} Subscribers - Daily EDM".format(key)
		contactListName = "Contacts - {} Subscribers - Daily EDM".format(key)
		consolidatedList = "Contacts - {} Subscribers - Consolidated".format(key)
		consolidatedSegment = "Segment - {} Subscribers - Consolidated".format(key)
		
		contactListID = getContactListId(contactListName, config.emarsysConfig['custCareAcct'])
		segmentId = getSegmentId(segmentName, config.emarsysConfig['custCareAcct']) 
		
		if all([contactListID, segmentId]):
			logging.info("Overwritting contacts in Emarsys for %s - Daily segment.", key)
			df_payments_package.columns = ["3", "34348", "35020", "31"]
			df_payments_package.drop('35020', axis=1, inplace=True)
			retCode = createContactsFromDataFrame(df_payments_package, config.emarsysConfig['custCareAcct'])
			totEmails = sum([len(i) for i in [json.loads(item)['data']['ids'] for item in retCode]])
			logging.info("%s contacts updated in Emarsys for %s - Daily segment.", totEmails, key)
			# retCode = overwriteContactListFromDataFrame(contactListName, df_payments_package, config.emarsysConfig['custCareAcct'])
			retCode = addContactsListFromDataFrame(contactListName, df_payments_package, config.emarsysConfig['custCareAcct'])
		else:
			retCode = deleteSegment(segmentId, config.emarsysConfig['custCareAcct'])
			retCode = deleteContactList(contactListID, config.emarsysConfig['custCareAcct'])
			df_payments_package.columns = ["3", "34348", "35020", "31"]
			df_payments_package.drop('35020', axis=1, inplace=True)
			retCode = createContactsFromDataFrame(df_payments_package, config.emarsysConfig['custCareAcct'])
			totEmails = sum([len(i) for i in [json.loads(item)['data']['ids'] for item in retCode]])
			logging.info("%s contacts updated in Emarsys for %s - Daily segment.", totEmails, key)
			retCode = createContactsListFromDataFrame(contactListName, df_payments_package, config.emarsysConfig['custCareAcct'])
			contactListID = json.loads(retCode)["data"]["id"]
			retCode = createSegment(segmentName, contactListID, config.emarsysConfig['custCareAcct'])
		
		df_payments_package.columns = ["3", "37470", "31"]
		df_payments_package.drop('31', axis=1, inplace=True)
		retCode = createContactsFromDataFrame(df_payments_package, config.emarsysConfig['tglAcct'])
		totEmails = sum([len(i) for i in [json.loads(item)['data']['ids'] for item in retCode]])
		logging.info("%s contacts updated in Emarsys for %s - Consolidated segment.", totEmails, key)
		retCode = addContactsListFromDataFrame(consolidatedList, df_payments_package, config.emarsysConfig['tglAcct'])
		contactListID = getContactListId(consolidatedList, config.emarsysConfig['tglAcct'])
		retCode = createSegment(consolidatedSegment, contactListID, config.emarsysConfig['tglAcct'])
		logging.info("Emarsys segment creation completed for the %s subscription.", key)
	
	logging.info("Emarsys segment creation Completed for the date %s.", processDate)
	logging.info("Creating Pivot table based on L30 transactions.")
	import adyen_purchase_pivot as adypvt
	xlFile = adypvt.execute(processDate)
	logging.info("Sending emailer to recepients.")
	import send_mail
	send_mail.send_email(df_payments, xlFile, processDate)
	logging.info("END OF PROGRAM")

if __name__ == "__main__": main()


