# project2.2


flume-ng agent -n agent1 -c conf -f /home/acadgild/apache-flume-1.6.0-bin/conf/filecopy.conf
REGISTER /usr/local/pig/lib/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

complaints =  LOAD 'consumer_complaints' using  CSVLoader(',') AS (Date_received:chararray,product:chararray,sub_product:chararray,issue:chararray,sub_issue:chararray,
consumer_complaint:chararray,puplic_response:chararray,company:chararray,state:chararray,
zip:double,sub_source:chararray,Date_sent:chararray,company_response:chararray,time_response:chararray,
consumer_disputed:chararray,complaint_id:double);

1. Filter_compliants = filter complaints by time_resposne = 'Yes';
   Num_complaints = FOREACH Filter_complaints GENERATE COUNT(*);
   Dump num_complaints;
   Store num_complaints into '/project2/pig/num_complaints';

   sqoop export -m 1 --connect jbdc:MYSQL://localhost/complaints --username acadgild --table complaint1 --export-dir /project2/pig/num_complaints/

2. Filter_forward = filter complaints by Date_received = Date_sent;
   Num_complaints_sameday = FOREACH Filter_forward GENERATE COUNT(*);
   dump num_complaints_sameday;
   Store num_complaints_sameday into '/project2/pig/num_complaints_sameday';

   sqoop export -m 1 --connect jbdc:MYSQL://localhost/complaints --username acadgild --table complaint2 --export-dir /project2/pig/num_complaints_sameday


3. group_chart = group complaints by company;
   Num_company = FOREACH group_chart GENERATE group,COUNT(complaints.consumer_complaint) as count; 
   Max_company = FOREACH num_company GENERATE complaints.company,MAX(count);
   dump Max_company;
  Store Max_company into '/project2/pig/Max_company';

   sqoop export -m 1 --connect jbdc:MYSQL://localhost/complaints --username acadgild --table complaint2 --export-dir /project2/pig/Max_company



4. Debit_colletion = filter complaints by product = 'Debit Collection';   
   Debit_year = FOREACH Debit_collection GENERATE ToDate(Date,'YYYY') as year,COUNT(consumer_complaint) as complaint;
   Year = Filter Debit_year by year = 2015;
   dump year;

 Store year into '/project2/pig/year';

   sqoop export -m 1 --connect jbdc:MYSQL://localhost/year --username acadgild --table complaint2 --export-dir /project2/pig/year
