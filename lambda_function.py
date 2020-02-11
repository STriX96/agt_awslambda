import boto3
import numpy as np


def lambda_handler(event, context):

    for record in event['Records']:
        #Getting the file that triggered the function
        bucketName = record['s3']['bucket']['name']
        fileKey = record['s3']['object']['key']
        s3 = boto3.client('s3')

        #Reading in the csv file (separated by ";")
        csvFile = s3.get_object(Bucket=bucketName, Key=fileKey)
        csvContent = csvFile['Body'].read().split()

        dates = []
        numbers = []

        #Variable to skip the header of the csv
        firstLine = True

        for row in csvContent:
            #Skipping header
            if firstLine:
                firstLine = False
                continue
            #Decoding from binary, splitting the row and add the values to lists
            splittedRow = row.decode().split(';')
            dates.append(splittedRow[0])
            numbers.append(int(splittedRow[1]))

        #Calculating averages of the datas
        #Dates should be in yyyy-mm-dd format for numpy to work (converting from other formats that we don't know in advance is really slow)
        avg_date = (np.array(dates, dtype='datetime64[D]').view('i8').mean().astype('datetime64[D]'))
        avg_num = sum(numbers) / len(numbers)

        #Printing to see the results in the log
        print("Average of dates: " + str(avg_date))
        print("Average of numbers: " + str(avg_num))

    return {
        'average_date': str(avg_date),
        'average_number': avg_num
    }
