from datetime import datetime
from datetime import timedelta

def lambda_handler(event, context):
    '''
    Generate a list of dates (string format)
    '''
    
    begin_date_str = "20180101"
    end_date_str = "20181231"
    
    current_date = datetime.strptime(begin_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")

    result = []

    while current_date <= end_date:
        current_date_str = current_date.strftime("%Y%m%d")

        result.append(current_date_str)
            
        # adding 1 day
        current_date += timedelta(days=1)
      
    return result