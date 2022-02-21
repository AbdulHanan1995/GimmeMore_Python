# Importing packages
import mysql.connector
import simplejson as json
import decimal

# Connection to the database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="gimmemore"
)


# Function to run query: (Query_Name)
def run_query(name, query):
    mycursor = mydb.cursor()
    mycursor.execute(query)
    rv = mycursor.fetchall()
    row_headers = [x[0] for x in mycursor.description]

    json_data = []
    for result in rv:
        json_data.append(dict(zip(row_headers, result)))
    jsons = json.dumps(json_data, indent=2)

    # Storing result locally in JSON
    with open(name + '.json', 'w') as f:
        f.write(jsons)


# Calling function for each query separately:
run_query("query_1","SELECT QUARTER(`registered_at`) AS Quarter,YEAR(`registered_at`) as Year,COUNT(`id`) AS Customers FROM customers WHERE QUARTER(CURDATE()) = QUARTER(`registered_at`) AND YEAR(CURDATE()) = YEAR(`registered_at`);")

run_query("query_2","SELECT`country` AS Country, CONCAT(LEFT(MONTHNAME(`registered_at`),3),' ',YEAR(CURDATE())) AS Months, COUNT(*) AS Customers FROM customers WHERE YEAR(CURDATE()) = YEAR(`registered_at`) GROUP BY Country, Months ORDER BY Customers DESC, Months LIMIT 10;")

run_query("query_3","SELECT `country` AS Country, COUNT(*) AS Visit FROM visits WHERE `visited_at` BETWEEN SUBDATE(CURDATE(), DAYOFWEEK(CURDATE())+5) AND SUBDATE(CURDATE(), DAYOFWEEK(CURDATE())-1) GROUP BY Country ORDER BY Visit DESC LIMIT 3;")

run_query("query_4","SELECT CONCAT(LEFT(MONTHNAME(`visited_at`),3),' ',YEAR(`visited_at`)) AS Month, `domain` AS Domain, `source` AS Sources, COUNT(*)/30 AS Avg_NoOfVisit FROM visits GROUP BY Month DESC, Domain, Sources;")

run_query("query_5","SELECT V.device AS Popular_Device, COUNT(V.device) AS Device_Visits FROM visits AS V INNER JOIN customers AS C ON C.id = V.customer_id WHERE V.visited_at > C.registered_at GROUP BY V.device ORDER BY COUNT(V.device) DESC LIMIT 1;")

run_query("query_6","SELECT `domain` AS Domain, COUNT(`domain`) AS Usages, COUNT(DISTINCT `customer_id`) AS Customers, SUM(`purchase_value`) AS Revenue, CONCAT(LEFT(MONTHNAME(CURDATE()),3), ' ' , YEAR(CURDATE())) AS Month FROM visits WHERE MONTH(CURDATE()) = MONTH(`visited_at`) GROUP BY Domain ORDER BY Usages DESC LIMIT 1;")