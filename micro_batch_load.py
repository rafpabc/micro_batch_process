
import mysql.connector
import pandas as pd
import numpy as np
from datetime import datetime
import time
import os


def stored_values(): #we use this function to query previously stored values in the table, so this process can be used not only when loading data into an empty table.
    sum_query = "SELECT SUM(price) FROM pragma_table;"
    valid_rows_query = "SELECT count(*) FROM pragma_table WHERE price IS NOT NULL;"
    min_query = "SELECT MIN(price) as min_price FROM pragma_table;"
    max_query = "SELECT MAX(price) as min_price FROM pragma_table;"
    with mysql.connector.connect(user='root', password='pragma_test',
                                    host='localhost',database='pragma_schema') as pragma_connection:
        cursor = pragma_connection.cursor()
        cursor.execute(sum_query)
        output_sum = cursor.fetchall()
        output_sum = output_sum[0][0]
        cursor.execute(valid_rows_query)
        output_divisor = cursor.fetchall()
        output_divisor = output_divisor[0][0]
        cursor.execute(min_query)
        output_min = cursor.fetchall()
        output_min = output_min[0][0]
        cursor.execute(max_query)
        output_max = cursor.fetchall()
        output_max = output_max[0][0]
    return output_sum,output_divisor,output_min,output_max

def read_micro_batch(output_sum,output_divisor,output_min,output_max,folder): #with this function i am making sure of just reading one microbatch at a time
    
    if output_sum is not None:
        sum_price = output_sum
    else:
        sum_price = float() #this object exists to store the cumulative sum of prices of all the microbatches, in this way, we can calculate the average of prices without resorting to read the whole table again
    if output_divisor != 0:
        divisor = output_divisor+1 #the plus one is to deal with the row im adding later in the function.
    else:
        divisor = 1 #the amount of prices we have included in the table is the denominator by which we have to divide the sum of prices. i dont use the index of the loop because we would be considering null values as 0s in this way, that would be incorrect, this is important.
    
    for x in range(0,len(folder)):
        micro_batch = pd.read_csv("C:/Users/usuario/Downloads/prueba_pragma/"+folder[x])
        micro_batch = micro_batch.astype(str)
        micro_batch = micro_batch.replace("nan", None)
        if ((x==0) and (output_min is not None)): #we only have to check one, if there is a min there has to be a max
            min_price = output_min
            max_price = output_max
        else:
            if x == 0:
                min_price = micro_batch.iloc[0,1]
                max_price = micro_batch.iloc[0,1] #i set this numbers just once, so i can begin calculating the min and max price of the whole series for the first record
            else:
                pass

        for i in range(0,len(micro_batch)):
            first_col = micro_batch.iloc[i,0] 
            second_col = micro_batch.iloc[i,1]
            third_col = micro_batch.iloc[i,2] #each row of each col is inserted one at a time
            with mysql.connector.connect(user='root', password='pragma_test',
                                host='localhost',database='pragma_schema') as pragma_connection: #i use this to open the connection to the database just when i need it and close it when once i have stopped using it
                cursor = pragma_connection.cursor()
                insert_query = "INSERT INTO pragma_table VALUES (%s,%s,%s);"
                rows_query = "SELECT COUNT(*) FROM pragma_table;"
                cursor.execute(insert_query,(first_col,second_col,third_col))
                pragma_connection.commit()
                cursor.execute(rows_query)
                row_count = cursor.fetchall()
                row_count = row_count[0][0]
                if second_col is not None: #i use this condition to deal with null values in the price column
                    sum_price = sum_price+float(second_col)
                    average_price = sum_price/divisor
                    divisor = divisor+1
                    if float(second_col) < float(min_price): #and, of course, these are to replace the min or max in case it is necessary
                        
                        min_price = float(second_col)
                    elif float(second_col) > float(max_price):
                        
                        max_price = float(second_col)
                    else:
                        pass
                else:
                    pass #the important here is that we are only storing a cumulative value, we are querying nothing from the table each time we need to produce these
                
                insert_query_stats = "INSERT INTO stats_table VALUES (%s,%s,%s,%s);"
                cursor.execute(insert_query_stats,(divisor-1,average_price,min_price,max_price))
                pragma_connection.commit() #i also created a table in the db to store the statistics values that is produced row by row, so the whole story of these can be queried if necessary

            print("Number of rows in table: "+str(row_count))
            print("Average price is: "+str(average_price))
            print("Minimum price is: "+str(min_price))
            print("Maximum price is: "+str(max_price)) #my preferred solution, as i understand the exercise is this, however, just displaying the values so the user can know what is happening in real time and then validate
            #but i think that having the other table can be useful to see what could had gone wrong in a given moment or review the whole story of the statistics.
            



            time.sleep(0.3) #im just adding this so the process can be seen as it is happening, because its so fast, however this can be commented out
        print("Connected to db: " + str(pragma_connection.is_connected()))
    return row_count,average_price,min_price,max_price

def validation(row_count,average_price,min_price,max_price):
    rows_query = "SELECT COUNT(*) FROM pragma_table;"
    avg_query = "SELECT AVG(price) as avg_price FROM pragma_table;"
    min_query = "SELECT MIN(price) as min_price FROM pragma_table;"
    max_query = "SELECT MAX(price) as min_price FROM pragma_table;"
    with mysql.connector.connect(user='root', password='pragma_test',
                                    host='localhost',database='pragma_schema') as pragma_connection: #i use this to open the connection to the database just when i need it and close it once i have stopped using it
        cursor = pragma_connection.cursor()
        cursor.execute(rows_query)
        output_rows = cursor.fetchall()
        output_rows = output_rows[0][0]
        cursor.execute(avg_query)
        output_avg = cursor.fetchall()
        output_avg = output_avg[0][0]
        cursor.execute(min_query)
        output_min = cursor.fetchall()
        output_min = output_min[0][0]
        cursor.execute(max_query)
        output_max = cursor.fetchall()
        output_max = output_max[0][0]
    if output_rows == row_count:
        print("Correct: last row count = " +str(row_count)+" and queried row count = "+str(output_rows))
    else:
        print("there was an error during load total rows inserted "+str(row_count)+" is not equal to rows in table") 
    if output_avg == average_price:
        print("Correct: Cumulative average "+str(average_price)+" equals queried average "+str(output_avg))
    else:
        print("there was an error during load, cumulative average "+str(average_price)+" is not equal to queried average") 
    if output_min == min_price:
        print("Correct: Last min price inserted "+str(min_price)+" equals queried minimum "+str(output_min))
    else:
        print("there was an error during load, last minimum price inserted "+str(min_price)+" is not equal to queried min") 
    if output_max == max_price:
        print("Correct: Last max price inserted "+str(max_price)+" equals queried minimum "+str(output_max))
    else:
        print("there was an error during load, last maximum price "+str(max_price)+" inserted is not equal to queried max")


folder = os.listdir("C:/Users/usuario/Downloads/prueba_pragma/") #here i am reading into a list all the files inside the folder in which i put the csv so i can iterate them later
folder = [x for x in folder if x != 'validation.csv'] 

validation(*read_micro_batch(*stored_values(),folder)) # first validation of pipeline

folder = os.listdir("C:/Users/usuario/Downloads/prueba_pragma/") #here i am reading into a list all the files inside the folder in which i put the csv so i can iterate them later
folder = [x for x in folder if x == 'validation.csv'] 

validation(*read_micro_batch(*stored_values(),folder))


def delete_everything(): #in case you want to start again from 0, uncomment the execution of this function.
    delete_query = "DELETE FROM pragma_table;" #you can change this table if you want to delete the values on the one that stores all the statistics
    with mysql.connector.connect(user='root', password='pragma_test',
                                    host='localhost',database='pragma_schema') as pragma_connection:
        cursor = pragma_connection.cursor()
        cursor.execute(delete_query)
        pragma_connection.commit()

#delete_everything()

def check_stats(): #in case you want to check the values stored outside the dbms you can use this
    select_query = "SELECT * FROM stats_table;" #you can change this table if you want to check the values on the other table
    with mysql.connector.connect(user='root', password='pragma_test',
                                    host='localhost',database='pragma_schema') as pragma_connection:
        cursor = pragma_connection.cursor()
        cursor.execute(select_query)
        select_output = cursor.fetchall()
        select_output = pd.DataFrame(select_output)
        select_output.columns = ['id','avg_price','min_price','max_price']
    return select_output

#check_stats()


