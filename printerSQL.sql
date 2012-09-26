drop table printer.printdetails;

create table printer.printdetails
   ( printDate timestamp,
     regNo varchar(15),
     document_title varchar(4000),
     printerName varchar(20),
     pagesPrinted numeric(4,0),
     cost numeric(4,2)
   );
   
drop table printer.prices;

create table printer.prices
	( printerName varchar(20),
	  cost_FirstPage numeric(2,2),
	  cost_perPage numeric(3,2),
	  extraCharges numeric(3,2)
	);

--drop table printer.lastLogExtraction;

--create table printer.lastLogExtraction
	--( previousLogExtraction timestamp
	--);

-- Report for max number of pages taken as printout
	--SELECT max(pagesPrinted) FROM printer.printdetails;
	
-- Report for total number of pages printed
	--SELECT count(pagesPrinted) FROM printer.printdetails;

-- Report for dates and total pages printed in that day
  --  SELECT date(printdate),sum(pagesPrinted) FROM printer.printdetails GROUP BY date(printdate);

--drop table printer.csgled;    
    
--create table printer.csgled
	--( rollno varchar(11),
	  --remarks varchar(40),
	  --drcr varchar(2),
	  --charges numeric(5,2),
	  --mon numeric(8,0),
	  --mo varchar(7)
	--);
CREATE SCHEMA printing;
DROP TABLE printing.csgled;
CREATE TABLE printing.csgled 
( rollno nvarchar(11), remarks nvarchar(40), drcr nvarchar(2), charges float(8), mon float(8), mo nvarchar(7));