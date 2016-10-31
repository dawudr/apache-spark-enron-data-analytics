# apache-spark-enron-data-analytics
Analysis of 200GB of Enron email Data on AWS using Apache Spark and JAVA

# The Problem 
Enron email data on AWS is big (210 GB), here I will be using Java and Apache Spark and native text/XML version of the email data files to answer the following questions: 

1. What is the average length, in words, of the emails? (Ignore attachments) 
2. Which are the top 100 recipient email addresses? (An email sent to N recipients would could N times - count “cc” as 50%) 

# Assumptions about the data
* To Email addresses taken from the first occurence of "To: ". And all the email addresses are comma separated on one line only.
* CC Email address taken from the first occurence of "CC: ". And all the email addresses are comma separated on one line only.
* Email address must be a valid email which satisfies this regular expression: ```([a-zA-Z0-9_\-\.]+)@([a-zA-Z0-9_\-\.]+)\.([a-zA-Z]{2,5}),?``` . All my regex validations done in my regex swiss army tool - in http://regex101.com
* For To email addresses I score them with 1, CC email addresses are scored with 0.5 to rank the top 100 most popular email address. Incidently I notice a lot of emails from wifes/family members in Enron...did they actually do any office work, I'm thinking....One even planned a 3 day wedding schedule on a Golf course!!
* Excluded the email headers when someone did a reply or forward from our word count.
* Words must satisfy this regular expression ```.?[a-zA-Z{-}?]+.?``` . Words are alphabet characters including hypenated words. They do not include numbers.
* Files ending with *.txt and *.eml are only considered and parsed.
* I strip headers by stripping out the start of the file to the end of line of the first occurence of ````X-ZLID:```` string. I've noticed this holds true for most of the native text email samples I examined.



# Getting started in 5 mins.

## Installation
1. Create an EC2 instance with sufficient RAM 2-4GB should be enough. The more the faster the emails will be processed.
2. Mount the EBS volume sized 210 GB with the snapshot ID snap-d203feb5 which contains Zip files of Enron email data.
3. Install JAVA 8 -
```
sudo yum install java-1.8.0
sudo yum install java-1.8.0-openjdk-devel
sudo /usr/sbin/alternatives --config java
sudo /usr/sbin/alternatives --config javac
```
This is important because the code uses Java 8 Streams and Lamdba functionality which is NOT supported in older Java versions.
4. Install MAVEN.
```
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven
mvn –version
```
5. Checkout this project from Github.
6. Run ```mvn clean install```   - this will compile and download all the Apache Spark, Hadoop, Test library dependencies defined in pom.xml.

## Unpack data files
1. Assuming the EBS volume of Enron email data has been mounted at ```/data````. Locate the 
2. Create a shell script to automate the batch decompressing of the data files into ```/data/test```. 
Use the following to create the shell script in your favourite Linux editor.
```
#!/bin/sh
echo "Decompressing email archives Zip files and copying to /data/test folder...."
for x in $(ls *_xml.zip); do
 dir=/data/test/${x%%.zip}
 echo "Running... sudo mkdir $dir"
 #sudo mkdir $dir
 echo "Running... unzip -d $dir $x"
 unzip -d $dir $x
done
````
3. Save the file and run sudo chmod a+x myUnzipShellScript.sh to give it execute permissions.
4. Run the shell script and this will unzip only the xml file ending with _xml.zip into ```/data/test``` where the filename will be used to create a subdirectory.

## Running the program
1. Remember ```java -version``` command must report Java JDK version 1.8.x.
2. Run ```java -jar target\enron-spark-1.0-SNAPSHOT.jar /data/test ``` to run the program.
3. You should see the following output for example:
```
PROCESSING EMAIL MESSAGE FILE(S)/FOLDER: /data/test/edrm-enron-v2_meyers
Setting up Apache Spark....

**************NOW PROCESSING 8231 EMAIL MESSAGE FILES**************
Please sit back and wait........
Setting up Java Thread Pools......
PROCESSING CURRENT EMAIL MESSAGE FILE: 
/data/test/edrm-enron-v2_meyers/xml_version/text_000/3.438368.PK3OFMOYVKRD4XSYR1TCA4RA45VWBGM1B.txt => Average word length: 4.688888888888889
...

***************************RESULTS***************************

************************TOP 100 EMAILS***********************
(pete.davis@enron.com,441.0)
(bert.meyers@enron.com,440.5)
(Geir.Solberg@enron.com,440.0)
(mark.guzman@enron.com,440.0)
(Craig.Dean@enron.com,440.0)
(bill.williams.III@enron.com,439.5)
(john.anderson@enron.com,439.0)
(michael.mier@enron.com,439.0)
(dporter3@enron.com,1.0)
(Eric.Linder@enron.com,1.0)
(monika.causholli@enron.com,1.0)
(leaf.harasin@enron.com,1.0)

Total Email Addresses count: 12

**************AVERAGE WORD LENGTH IN ALL EMAILS**************
Average word length: 6.014763070271431

*************************JOB SUMMARY*************************
Date started: Fri Oct 28 16:15:07 BST 2016
Date Ended: Fri Oct 28 16:35:04 BST 2016
Total Number of Email Messages scanned: 8231
Total time taken: 1196926ms

Stopping Apache Spark...
```
## Viewing the Apache Spark Console
4. Navigate to http://localhost:4040/jobs/ This load up the Apache Spark console with shows the status of all the data jobs running. Theres nothing that needs to be installed for this console, its bundled with the Java libraries and is only available while the programming is actively processing away.

![Apache Spark Running Console](https://github.com/dawudr/apache-spark-enron-data-analytics/raw/master/Console_Apache_Spark_Jobs_at_locahost_port_4040.png "Viewing the Apache Spark Console on http://localhost:4040")



