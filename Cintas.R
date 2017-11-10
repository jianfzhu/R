###--- 1 数据转换 ---###
setwd("D:\\R\\cintas")
library(XLConnect)
library(reshape2)
library(sca)
wb = loadWorkbook("Loss201708291455136430.xlsx")
df = readWorksheet(wb,sheet='sheet1',header=TRUE)
df = readWorksheet(wb,sheet=1,header=TRUE)
result = dcast(df,CUSTOMERNUMBER+NAME~P_ID)
# N/A 用0代替
result[is.na(result)]<-0
c1 <- result[3]/(result[3]+result[4])
result <- cbind(result,c1)
names(result) <- c('CUSTOMERNUMBER','NAME','DC','STK','Percent')
write.csv(result,file="20170405.csv",row.names=TRUE)


###---2 行列转换---###

setwd("D://R//cintas")
library(XLConnect)
library(reshape2)
library(sca)
wb = loadWorkbook("FY16.xlsx")
df = readWorksheet(wb,sheet='sheet1',header=TRUE)
result = dcast(df,Customer.Code+Customer~WEEK)
write.csv(result,file="FY16.csv",row.names=TRUE)


###---3 查询Wash Total---###
Sys.setenv(NLS_LANG="SIMPLIFIED CHINESE_CHINA.ZHS16GBK")
setwd("D://R//cintas//LR_Wash")
library(ROracle)
library(sqldf)
library(dplyr)
drv <- dbDriver("Oracle")
conn <- dbConnect(drv, username="REPORTING", password="REPORTING",
                  dbname="192.168.1.101:1521/ABSPR",DBMSencoding="utf8")
#dbSendQuery(conn, "SET NAMES utf8");
#config=read.table("config.txt",header= F,  sep="=")
CUSTOMERNUMBER =10047
INVOICEDATE1 ="2015-10-14"
INVOICEDATE2 ="2017-10-18"
FILENAME = paste0(getwd(),'/export/',CUSTOMERNUMBER,'_LR.xlsx')

#--查询 PrimaryID
# ctrl+shift+c 对行注释
# sql1 = paste0("SELECT DISTINCT Z.*,SL.CODE FROM(
# SELECT  BU.DESCRIPTION as LOCATION,IH.BILLTOCUSTOMERNUMBER,IH.BILLTOCUSTOMERNAME,IH.INVOICEDATE,IH.INVOICENUMBER,OH.PRIMARYID,IP.WEARERNUMBER,IP.WEARERNAME,IL.DESCRIPTION, IL.CODE PRODUCT,OH.SIZEDEFINITION_LINK_ID
#               FROM abssolute.INVOICELINE IL
#               LEFT JOIN abssolute.INVOICEHEADER IH ON IH.INVOICEHEADER_ID = IL.INVOICEHEADER_ID
#               LEFT JOIN abssolute.INVOICELINETYPE ILT ON ILT.INVOICELINETYPE_ID = IL.INVOICELINETYPE_ID
#               LEFT JOIN abssolute.BUSINESSUNIT BU ON BU.BUSINESSUNIT_ID = IL.PROCESSINGBUSINESSUNIT_ID
#               LEFT JOIN abssolute.INVOICELINEPRODUCT IP ON IP.INVOICELINE_ID = IL.INVOICELINE_ID
#               LEFT JOIN abssolute.OUTOFCIRCULATIONHISTORY OH ON OH.CUSTOMER_LINK_ID = IH.BILLTOCUSTOMER_LINK_ID
#               AND OH.OUTOFCIRCYEAR = TO_NUMBER(TO_CHAR(SYSDATE, 'yyyy'))
#               AND OH.WEAREREMPLOYMENT_LINK_ID = IP.WEAREREMPLOYMENT_LINK_ID
#               AND OH.PRODUCT_LINK_ID = IP.PRODUCT_LINK_ID
#               AND OH.SIZEDEFINITION_LINK_ID = IP.SIZEDEFINITION_LINK_ID
#               AND OH.TIMESTAMP > IH.INVOICEDATE
#               WHERE IL.INVOICELINETYPE_ID = 9
#               AND IL.EXTEND = 'Y'
#               AND IL.AMOUNT > 0
#               AND OH.REASONCODE_ID <> 7
#               AND IH.INVOICEDATE >= TO_DATE('",INVOICEDATE1,"', 'yyyy-mm-dd')
#               AND IH.INVOICEDATE <= TO_DATE('",INVOICEDATE2,"', 'yyyy-mm-dd')
#               AND IH.BILLTOCUSTOMERNUMBER = '",CUSTOMERNUMBER,"'
#               ORDER BY IL.INVOICELINETYPE_ID, IP.WEARERNUMBER ) Z LEFT JOIN abssolute.SIZEDEFINITION_LINK SL ON Z.SIZEDEFINITION_LINK_ID = SL.SIZEDEFINITION_LINK_ID")

sql1 = paste0("SELECT DISTINCT BU.DESCRIPTION  AS  LOCATION,
	            IH.BILLTOCUSTOMERNUMBER, IH.BILLTOCUSTOMERNAME, 
              TO_CHAR(IH.INVOICEDATE,'YYYY-MM-DD') INVOICEDATE,
              IH.INVOICENUMBER,
              OH.PRIMARYID,
              IP.WEARERNUMBER, IP.WEARERNAME, 
              IL.DESCRIPTION, IL.CODE PRODUCT,
              SL.CODE SIZE_CODE, 
              PR.REPPRICE PRICE, 
              ST.FACTORYDESCRIPTION   REASON,
              RAG.WASHESTOTAL,RAG.REPAIRSTOTAL,
              RAG.REWASHTOTAL, RAG.FIRSTISSUEDATE, RAG.LASTISSUEDATE
              FROM ABSSOLUTE.INVOICELINE IL
              LEFT JOIN ABSSOLUTE.INVOICEHEADER IH
              ON IH.INVOICEHEADER_ID = IL.INVOICEHEADER_ID
              LEFT JOIN ABSSOLUTE.INVOICELINETYPE ILT
              ON ILT.INVOICELINETYPE_ID = IL.INVOICELINETYPE_ID
              LEFT JOIN ABSSOLUTE.BUSINESSUNIT BU
              ON BU.BUSINESSUNIT_ID = IL.PROCESSINGBUSINESSUNIT_ID
              LEFT JOIN ABSSOLUTE.INVOICELINEPRODUCT IP
              ON IP.INVOICELINE_ID = IL.INVOICELINE_ID
              LEFT JOIN ABSSOLUTE.OUTOFCIRCULATIONHISTORY OH
              ON OH.CUSTOMER_LINK_ID = IH.BILLTOCUSTOMER_LINK_ID
              AND OH.OUTOFCIRCYEAR = TO_NUMBER(TO_CHAR(SYSDATE, 'YYYY'))
              AND OH.WEAREREMPLOYMENT_LINK_ID = IP.WEAREREMPLOYMENT_LINK_ID
              AND OH.PRODUCT_LINK_ID = IP.PRODUCT_LINK_ID
              AND OH.SIZEDEFINITION_LINK_ID = IP.SIZEDEFINITION_LINK_ID
              AND OH.TIMESTAMP > IH.INVOICEDATE
              LEFT JOIN ABSSOLUTE.SIZEDEFINITION_LINK SL
              ON OH.SIZEDEFINITION_LINK_ID = SL.SIZEDEFINITION_LINK_ID
              LEFT JOIN ABSSOLUTE.RAGGEDITEM RAG
              ON OH.PRIMARYID = RAG.PRIMARYID
              LEFT JOIN ABSSOLUTE.INVOICELINESUBTYPE ST
              ON IL.INVOICELINESUBTYPE = ST.SUBTYPE
              AND IL.INVOICELINETYPE_ID = ST.INVOICELINETYPE_ID
              LEFT JOIN (SELECT C.CUSTOMERNUMBER,M.CODE,
              SUBSTR(M.PRICE, 1, INSTR(M.PRICE, ',', 1, 1) - 1) PRICE,
              SUBSTR(M.PRICE,INSTR(M.PRICE, ',', 1, 1) + 1, INSTR(M.PRICE, ',', 1, 2) - INSTR(M.PRICE, ',', 1, 1) - 1) REPPRICE
              FROM ABSSOLUTE.MV_PRODUCTPERCUSTOMER M, ABSSOLUTE.CUSTOMER C
              WHERE M.CUSTOMER_ID = C.CUSTOMER_ID) PR
              ON PR.CUSTOMERNUMBER = IH.DELTOCUSTOMERNUMBER
              AND IL.CODE = PR.CODE
              WHERE IL.INVOICELINETYPE_ID = 9
              AND IL.EXTEND = 'Y'
              AND IL.AMOUNT > 0
              AND OH.REASONCODE_ID <> 7
              AND IH.INVOICEDATE >= TO_DATE('",INVOICEDATE1,"', 'yyyy-mm-dd')
              AND IH.INVOICEDATE <= TO_DATE('",INVOICEDATE2,"', 'yyyy-mm-dd')
              AND IH.BILLTOCUSTOMERNUMBER =  '",CUSTOMERNUMBER,"'
              ORDER BY  IP.WEARERNUMBER DESC
              ")

rs1 <- dbSendQuery(conn, sql1)
data1 <- fetch(rs1)

#--查询UNIQUEITME_LINK_ID
sql2 = paste0("select uil.UNIQUEITEM_LINK_ID,ri.RAGGEDITEM_ID,ri.PRIMARYID,ri.WASHESTOTAL from abssolute.raggeditem ri,abssolute.uniqueitem_link uil 
              where ri.raggeditem_id = uil.raggeditem_id
              and ri.PRIMARYID in(
              SELECT distinct OH.PRIMARYID
              FROM abssolute.INVOICELINE IL
              LEFT JOIN abssolute.INVOICEHEADER IH
              ON IH.INVOICEHEADER_ID = IL.INVOICEHEADER_ID
              LEFT JOIN abssolute.INVOICELINETYPE ILT
              ON ILT.INVOICELINETYPE_ID = IL.INVOICELINETYPE_ID
              LEFT JOIN abssolute.BUSINESSUNIT BU
              ON BU.BUSINESSUNIT_ID = IL.PROCESSINGBUSINESSUNIT_ID
              LEFT JOIN abssolute.INVOICELINEPRODUCT IP
              ON IP.INVOICELINE_ID = IL.INVOICELINE_ID
              LEFT JOIN abssolute.OUTOFCIRCULATIONHISTORY OH
              ON OH.CUSTOMER_LINK_ID = IH.BILLTOCUSTOMER_LINK_ID
              AND OH.OUTOFCIRCYEAR = TO_NUMBER(TO_CHAR(SYSDATE, 'yyyy'))
              AND OH.WEAREREMPLOYMENT_LINK_ID = IP.WEAREREMPLOYMENT_LINK_ID
              AND OH.PRODUCT_LINK_ID = IP.PRODUCT_LINK_ID
              AND OH.SIZEDEFINITION_LINK_ID = IP.SIZEDEFINITION_LINK_ID
              AND OH.TIMESTAMP > IH.INVOICEDATE
              LEFT JOIN abssolute.INVOICELINESUBTYPE ST
              ON IL.INVOICELINESUBTYPE = ST.SUBTYPE
              AND IL.INVOICELINETYPE_ID = ST.INVOICELINETYPE_ID
              LEFT JOIN (SELECT C.CUSTOMERNUMBER,
              M.CODE,
              SUBSTR(M.PRICE, 1, INSTR(M.PRICE, ',', 1, 1) - 1) PRICE,
              SUBSTR(M.PRICE,
              INSTR(M.PRICE, ',', 1, 1) + 1,
              INSTR(M.PRICE, ',', 1, 2) -
              INSTR(M.PRICE, ',', 1, 1) - 1) REPPRICE
              FROM abssolute.MV_PRODUCTPERCUSTOMER M, abssolute.CUSTOMER C
              WHERE M.CUSTOMER_ID = C.CUSTOMER_ID) PR
              ON PR.CUSTOMERNUMBER = IH.DELTOCUSTOMERNUMBER
              AND IL.CODE = PR.CODE
              WHERE IL.INVOICELINETYPE_ID = 9
              AND IL.EXTEND = 'Y'
              AND IL.AMOUNT > 0
              AND OH.REASONCODE_ID <> 7
              AND IH.INVOICEDATE >= TO_DATE('",INVOICEDATE1,"', 'yyyy-mm-dd')
              AND IH.INVOICEDATE <= TO_DATE('",INVOICEDATE2,"', 'yyyy-mm-dd')
              AND IH.BILLTOCUSTOMERNUMBER =  '",CUSTOMERNUMBER,"'
              )")

rs2 <- dbSendQuery(conn, sql2)
data2 <- fetch(rs2)

x <- sqldf("select PRIMARYID from data1 where PRIMARYID not in (select PRIMARYID from data2 )")

sql11 = paste0("SELECT U.PRIMARYID,U.WASHESTOTAL,U.REPAIRSTOTAL,U.REWASHTOTAL,UFI.FIRSTISSUEDATE,UFI.LASTISSUEDATE FROM ABSSOLUTE.UNIQUEITEM U,ABSSOLUTE.UNIQUEITEMFIXEDINFO UFI
                WHERE U.PRIMARYID IN
                (", paste0(shQuote(x[,1], 'sh'), collapse = ','), ")
                AND U.UNIQUEITEM_ID = UFI.UNIQUEITEM_ID
              ")
rs11 <- dbSendQuery(conn, sql11)
data11 <- fetch(rs11)

data12 <- sqldf("select * from data1 where PRIMARYID  in (select PRIMARYID from x )")
data12 <- data12[,1:13]
data13 <- merge(data12, data11, all=TRUE)
data13 <- data13[,c(2:6,1,7:18)]
data1 <- sqldf("select * from data1 where PRIMARYID  not in (select PRIMARYID from x )")
data1 <- rbind(data1,data13)

sql21 = paste0("select uil.UNIQUEITEM_LINK_ID,ri.uniqueITEM_ID,ri.PRIMARYID,ri.WASHESTOTAL from abssolute.uniqueitem ri,abssolute.uniqueitem_link uil 
              where ri.uniqueitem_id = uil.uniqueitem_id
              and ri.PRIMARYID in
              (", paste0(shQuote(x[,1], 'sh'), collapse = ','), ")
              ")

rs21 <- dbSendQuery(conn, sql21)
data21 <- fetch(rs21)

names(data21) <- c('UNIQUEITEM_LINK_ID','RAGGEDITEM_ID','PRIMARYID','WASHESTOTAL')
data2 <- rbind(data2,data21)

remove(data11,data12,data13,data21)
#--查询Wash次数
sql3 = paste0("select s.uniqueitem_link_id,count(1) as WASHESTOTAL_2
              from abssolute.scan s,abssolute.transactiontype tt
              where s.transactiontype_id IN (1 , 4)
              and tt.transactiontype_id = s.transactiontype_id     
              and s.uniqueitem_link_id IN 
              (", paste0(shQuote(data2[,1], 'sh'), collapse = ','), ")
              group by s.uniqueitem_link_id
              ")

rs3 <- dbSendQuery(conn, sql3)
data3 <- fetch(rs3)

#--关联PRIMARYID和WASH次数
result <- merge(data2, data3, all=TRUE)
result[is.na(result)] <- 0

#--关联人员和WASH次数
result <- merge(data1, result, all=TRUE)
result <- result[order(result$INVOICEDATE,result$WEARERNUMBER),]

result[,c(1,2,21)]
#--写入文件
#write.csv(result[,c(3:14,1:2)],file=FILENAME,row.names=FALSE)
library(xlsx)
write.xlsx(x = result[,c(3:14,1:2,21)], file = FILENAME, sheetName = "Sheet1", row.names = FALSE)
dbDisconnect(conn)


###---4 查询Wash Total根据Primary ID---###
setwd("D://R//cintas")
library(ROracle)
drv <- dbDriver("Oracle")
conn <- dbConnect(drv, username="REPORTING", password="REPORTING",
                  dbname="192.168.1.101:1521/ABSPR")
data1=read.csv("20170405.csv",header= T,  sep=",", encoding = "UTF-8")

FILENAME = paste0(getwd(),'/export/20170405_10048_WT.csv')
#--查询 PrimaryID

#--查询UNIQUEITME_LINK_ID ①for Ragged --
sql21 = paste0("select uil.UNIQUEITEM_LINK_ID,ri.RAGGEDITEM_ID,ri.PRIMARYID,ri.WASHESTOTAL from abssolute.raggeditem ri,abssolute.uniqueitem_link uil 
              where ri.raggeditem_id = uil.raggeditem_id
              and ri.PRIMARYID in(",paste0(shQuote(data1[2001:2568,4], 'sh'), collapse = ','),")")

#--查询UNIQUEITME_LINK_ID ②for unique --
sql22 = paste0("select uil.UNIQUEITEM_LINK_ID,ui.uniqueitem_id,ui.PRIMARYID,ui.WASHESTOTAL from abssolute.uniqueitem ui,abssolute.uniqueitem_link uil 
              where ui.uniqueitem_id = uil.uniqueitem_id
              and ui.PRIMARYID in(",paste0(shQuote(data1[2001:2568,4], 'sh'), collapse = ','),")")              

rs21 <- dbSendQuery(conn, sql21)
data21 <- fetch(rs21)
data2 <- rbind(data2,data21[,-2])
rs22 <- dbSendQuery(conn, sql22)
data22 <- fetch(rs22)
data2 <- rbind(data2,data22[,-2])

#--合并UNIQUEITME_LINK_ID数据 --
data2 <- rbind(data21[,-2],data22[,-2])

#--查询Wash次数
sql31 = paste0("select s.uniqueitem_link_id,count(1) as WASHESTOTAL_2
              from abssolute.scan s,abssolute.transactiontype tt
              where s.transactiontype_id IN (1 , 4)
              and tt.transactiontype_id = s.transactiontype_id     
              and s.uniqueitem_link_id IN 
              (", paste0(shQuote(data2[2001:2606,1], 'sh'), collapse = ','), ")
              group by s.uniqueitem_link_id
              ")

rs31 <- dbSendQuery(conn, sql31)
data31 <- fetch(rs31)
data32 <- fetch(rs31)
data3 <- rbind(data31,data32)
data3 <- rbind(data3,data31)

#--关联PRIMARYID和WASH次数
result1 <- merge(data2, data3, all=TRUE)
result2 <- merge(data2, data3, all=TRUE)
result3 <- merge(data2, data3, all=TRUE)

result <- rbind(result1,result2,result3)
result1[is.na(result1)] <- 0

#--关联人员和WASH次数
result <- merge(data1, result1, by.x="PRIMARYID" , by.y= "PRIMARYID",all.x=TRUE)
#result <- result[order(result$WEARERNUMBER),]

#--写入文件
#write.csv(result[,c(2:10,1,13:14)],file=FILENAME,row.names=TRUE)
write.csv(result,file=FILENAME,row.names=TRUE)
dbDisconnect(conn)
data_name=read.csv2("20170405_name.txt",header= T,  sep=",", encoding = "UTF-8")


###---5 数据转换妮维雅 ---###
setwd("D:\\R\\cintas")
library(XLConnect)
library(reshape2)
library(sca)
library(plyr)
wb = loadWorkbook("m.xlsx")
df = readWorksheet(wb,sheet=1,header=TRUE)
#head(df)
x <- ddply(df,.(Wearer.,Name,Admin., Date.Active, Department),summarise,bank=paste(bank, collapse=','))
df[order(df[,1-3]),]
df <- arrange(df, Wearer, Way)
x <- ddply(df,.(Wearer,Name),summarise,Method=paste(Way, collapse=','))
write.csv(x,file="m.csv",row.names=TRUE)


###----6 ---###
setwd("D:\\R\\cintas")
library(XLConnect)
library(reshape2)
library(sca)
library(plyr)
wb = loadWorkbook("nwy.xlsx")
df = readWorksheet(wb,sheet=1,header=TRUE)
df2 = readWorksheet(wb,sheet=2,header=TRUE)
data2 <- df2[df2$Max!=0,]
df2 <- data2[data2$Finishing.Method!=99,c(1,3,4,11,12)]
df <- df[order(df$Wearer,df$Product,df$Size,df$Max),]
df2 <- df2[order(df2$Wearer, df2$Product, df2$Size, df2$Max),]
result <- merge(x=df, y=df2, by=c("Wearer","Product","Size","Max") ,all=TRUE)
write.csv(result,file="nwy_new.csv",row.names=TRUE)


###----7 柜子使用信息 2017/6/7/-----###
setwd("D:\\R\\cintas")
library(ROracle)
drv <- dbDriver("Oracle")
conn <- dbConnect(drv, username="REPORTING", password="REPORTING",
                  dbname="192.168.1.101:1521/ABSPR")
library(XLConnect)
library(reshape2)
library(sca)
library(plyr)

sql = paste0("SELECT C.CUSTOMERNUMBER,C.NAME,
              CASE WHEN L.LOCKERSTATUS = 1 THEN 'AVAILABLE'
              WHEN L.LOCKERSTATUS =2  THEN 'USED'
              ELSE 'TEMP' END LOCKERSTATUS,
              COUNT(*) AS CNT FROM ABSSOLUTE.LOCKER L,ABSSOLUTE.CUSTOMER C
              WHERE C.DATEINACTIVE > sysdate
                AND C.DEFAULTBUSINESSUNIT_ID =1
                AND L.CUSTOMER_ID = C.CUSTOMER_ID
                GROUP BY C.CUSTOMERNUMBER,C.NAME,L.LOCKERSTATUS
                ORDER BY C.CUSTOMERNUMBER,C.NAME,L.LOCKERSTATUS")

rs <- dbSendQuery(conn, sql)
df <- fetch(rs)

#wb = loadWorkbook("20170607_lockstatus_details.xlsx")
#df = readWorksheet(wb,sheet=1,header=TRUE)
result = dcast(df,CUSTOMERNUMBER+NAME~LOCKERSTATUS)

# N/A 用0代替
result[is.na(result)]<-0
c1 <- result[5]/(result[3]+result[4]+result[5])
result <- cbind(result,c1)
names(result) <- c('CUSTOMERNUMBER','NAME','available','Temporary','used','used %')
result1 <- result[,c(1,2,3,5,4,6)]
write.csv(result1,file="lockstatus.csv",row.names=TRUE)


###----8 根据Priary ID 查询员工信息----###
setwd("D:\\R\\cintas")
library(XLConnect)
library(ROracle)
drv <- dbDriver("Oracle")
conn <- dbConnect(drv, username="REPORTING", password="REPORTING",
                  dbname="192.168.1.101:1521/ABSPR")
wb <- loadWorkbook("3rd_10033.xlsx")
ID <- readWorksheet(wb,sheet=1,header=TRUE)

sql = paste0("SELECT C.CUSTOMERNUMBER,C.NAME,W.WEARERNUMBER,W.FULLNAME,W.DATEACTIVE,W.DATEINACTIVE,U.PRIMARYID,U.WASHESTOTAL,P.CODE,PD.DESCRIPTION,S.CODE AS SIZE_CODE,UF.FIRSTISSUEDATE,U.LASTOUTSCANDATE,U.LASTSCANDATE
FROM ABSSOLUTE.UNIQUEITEM U,ABSSOLUTE.UNIQUEITEMFIXEDINFO UF,ABSSOLUTE.WEAREREMPLOYMENT WP,ABSSOLUTE.PRODUCT P,ABSSOLUTE.PRODUCT_DESC PD,ABSSOLUTE.SIZEDEFINITION S,ABSSOLUTE.WEARER W,
             ABSSOLUTE.CUSTOMER C,ABSSOLUTE.UNIQUEITEMNONPOOL UN
             WHERE U.UNIQUEITEM_ID=UF.UNIQUEITEM_ID
             AND U.UNIQUEITEM_ID=UN.UNIQUEITEM_ID
             AND UN.WEAREREMPLOYMENT_ID=WP.WEAREREMPLOYMENT_ID
             AND U.PRODUCT_ID=P.PRODUCT_ID
             AND P.PRODUCT_ID=PD.PRODUCT_ID
             AND PD.LANGUAGE_ID=1
             AND U.SIZEDEFINITION_ID=S.SIZEDEFINITION_ID
             AND W.WEARER_ID=WP.WEARER_ID
             AND W.CUSTOMER_ID=C.CUSTOMER_ID
             AND U.PRIMARYID IN
              (", paste0(shQuote(ID[,1], 'sh'), collapse = ','), ")
             ORDER BY C.CUSTOMER_ID,W.WEARERNUMBER")

rs <- dbSendQuery(conn, sql)
df <- fetch(rs)
write.csv(df,file="10033.csv",row.names=TRUE)
library(xlsx)
write.xlsx(x = df, file = "3rd_10033_info.xlsx",
           sheetName = "Sheet1", row.names = FALSE)


###---9 查询10048衣服状态---###
setwd("D:\\R\\cintas")
library(ROracle)
drv <- dbDriver("Oracle")
conn <- dbConnect(drv, username="REPORTING", password="REPORTING",
                  dbname="192.168.1.101:1521/ABSPR")
library(XLConnect)
library(reshape2)
library(sca)
library(plyr)

sql = paste0("SELECT C.CUSTOMERNUMBER,W.WEARERNUMBER,W.FULLNAME,W.DATEACTIVE,U.PRIMARYID,U.WASHESTOTAL,P.CODE,PD.DESCRIPTION,S.CODE AS SIZE_CODE,UF.FIRSTISSUEDATE,UF.LASTISSUEDATE,U.LASTOUTSCANDATE,U.LASTSCANDATE,SD.DESCRIPTION
FROM ABSSOLUTE.UNIQUEITEM U,ABSSOLUTE.UNIQUEITEMFIXEDINFO UF,ABSSOLUTE.WEAREREMPLOYMENT WP,ABSSOLUTE.PRODUCT P,ABSSOLUTE.PRODUCT_DESC PD,ABSSOLUTE.SIZEDEFINITION S,ABSSOLUTE.WEARER W,
             ABSSOLUTE.CUSTOMER C,ABSSOLUTE.UNIQUEITEMNONPOOL UN,ABSSOLUTE.STAY_DESC SD
             WHERE C.CUSTOMERNUMBER=10047
             AND U.STATUS_ID=26
             AND U.STAY_ID=SD.STAY_ID
             AND SD.LANGUAGE_ID=1
             AND W.DATEINACTIVE>=SYSDATE
             AND U.UNIQUEITEM_ID=UF.UNIQUEITEM_ID
             AND U.UNIQUEITEM_ID=UN.UNIQUEITEM_ID
             AND UN.WEAREREMPLOYMENT_ID=WP.WEAREREMPLOYMENT_ID
             AND U.PRODUCT_ID=P.PRODUCT_ID
             AND P.PRODUCT_ID=PD.PRODUCT_ID
             AND PD.LANGUAGE_ID=1
             AND U.SIZEDEFINITION_ID=S.SIZEDEFINITION_ID
             AND W.WEARER_ID=WP.WEARER_ID
             AND W.CUSTOMER_ID=C.CUSTOMER_ID
             ORDER BY W.WEARERNUMBER,P.CODE,S.CODE")

rs <- dbSendQuery(conn, sql)
df <- fetch(rs)

#--查询UNIQUEITME_LINK_ID ①for Ragged --
sql21 = paste0("select uil.UNIQUEITEM_LINK_ID,ri.RAGGEDITEM_ID,ri.PRIMARYID,ri.WASHESTOTAL from abssolute.raggeditem ri,abssolute.uniqueitem_link uil 
               where ri.raggeditem_id = uil.raggeditem_id
               and ri.PRIMARYID in(",paste0(shQuote(df[1:1000,5], 'sh'), collapse = ','),")")

#--查询UNIQUEITME_LINK_ID ②for unique --
sql22 = paste0("select uil.UNIQUEITEM_LINK_ID,ui.uniqueitem_id,ui.PRIMARYID,ui.WASHESTOTAL from abssolute.uniqueitem ui,abssolute.uniqueitem_link uil 
               where ui.uniqueitem_id = uil.uniqueitem_id
               and ui.PRIMARYID in(",paste0(shQuote(df[1:1000,5], 'sh'), collapse = ','),")")              

rs21 <- dbSendQuery(conn, sql21)
data21 <- fetch(rs21)

rs22 <- dbSendQuery(conn, sql22)
data22 <- fetch(rs22)

#--合并UNIQUEITME_LINK_ID数据 --
data2 <- rbind(data21[,-2],data22[,-2])

#--查询Wash次数
sql31 = paste0("select s.uniqueitem_link_id,count(1) as WASHESTOTAL_2
               from abssolute.scan s,abssolute.transactiontype tt
               where s.transactiontype_id IN (1 , 4)
               and tt.transactiontype_id = s.transactiontype_id     
               and s.uniqueitem_link_id IN 
               (", paste0(shQuote(data2[1:1000,1], 'sh'), collapse = ','), ")
               group by s.uniqueitem_link_id
               ")

rs31 <- dbSendQuery(conn, sql31)
data3 <- fetch(rs31)

#--关联PRIMARYID和WASH次数
result1 <- merge(data2, data3, all=TRUE)
result1[is.na(result1)] <- 0

#--关联人员和WASH次数
result <- merge(df, result1, by.x="PRIMARYID" , by.y= "PRIMARYID",all.x=TRUE)
dbDisconnect(conn)
#--写入文件
library(xlsx)
write.xlsx(x = result, file = "20170628_10048_WT.xlsx",
           sheetName = "Sheet1", row.names = FALSE)


###---10 查询10048衣服状态---###
setwd("D:\\R\\cintas\\LR_Wash")
library(ROracle)
drv <- dbDriver("Oracle")
conn <- dbConnect(drv, username="REPORTING", password="REPORTING",
                  dbname="192.168.1.101:1521/ABSPR")
library(XLConnect)
library(reshape2)
library(sca)
library(plyr)

wb = loadWorkbook("10048_uniqueitem.xlsx")
df = readWorksheet(wb,sheet=1,header=TRUE)


#--合并UNIQUEITME_LINK_ID数据 --
data2 <- data.frame(df[,1])

#--查询Wash次数
sql31 = paste0("select s.uniqueitem_link_id,count(1) as WASHESTOTAL_2
               from abssolute.scan s,abssolute.transactiontype tt
               where s.transactiontype_id IN (1 , 4)
               and tt.transactiontype_id = s.transactiontype_id     
               and s.uniqueitem_link_id IN 
               (", paste0(shQuote(data2[2001:2623,1], 'sh'), collapse = ','), ")
               group by s.uniqueitem_link_id
               ")

rs31 <- dbSendQuery(conn, sql31)
data3 <- fetch(rs31)
data4 <- fetch(rs31)
data5 <- fetch(rs31)

data6 <- rbind(data3,data4,data5)

#--关联人员和WASH次数
result <- merge(df, data6, by.x="UNIQUEITEM_ID" , by.y= "UNIQUEITEM_LINK_ID",all.x=TRUE)
result2 <- result[order(result$DATEINACTIVE,result$WEARERNUMBER),]
dbDisconnect(conn)
#--写入文件
library(xlsx)
write.xlsx(x = result, file = "20170616_10048_WT.xlsx",
           sheetName = "Sheet1", row.names = FALSE)


###---11. 查询衣服清洗次数  ---###
setwd("D:\\R\\cintas")
library(ROracle)
drv <- dbDriver("Oracle")
conn <- dbConnect(drv, username="REPORTING", password="REPORTING",
                  dbname="192.168.1.101:1521/ABSPR")
library(XLConnect)
library(reshape2)
library(sca)
library(dplyr)
CUSTOMERNUMBER =10047

sql = paste0("SELECT C.CUSTOMERNUMBER,W.WEARERNUMBER,W.FULLNAME,W.DATEACTIVE,U.PRIMARYID,U.WASHESTOTAL,P.CODE,PD.DESCRIPTION,S.CODE AS SIZE_CODE,UF.FIRSTISSUEDATE,UF.LASTISSUEDATE,U.LASTOUTSCANDATE,U.LASTSCANDATE,SD.DESCRIPTION
             FROM ABSSOLUTE.UNIQUEITEM U,ABSSOLUTE.UNIQUEITEMFIXEDINFO UF,ABSSOLUTE.WEAREREMPLOYMENT WP,ABSSOLUTE.PRODUCT P,ABSSOLUTE.PRODUCT_DESC PD,ABSSOLUTE.SIZEDEFINITION S,ABSSOLUTE.WEARER W,
             ABSSOLUTE.CUSTOMER C,ABSSOLUTE.UNIQUEITEMNONPOOL UN,ABSSOLUTE.STAY_DESC SD
             WHERE C.CUSTOMERNUMBER=",CUSTOMERNUMBER,"
             AND U.STATUS_ID=26
             AND U.STAY_ID=SD.STAY_ID
             AND SD.LANGUAGE_ID=1
             AND W.DATEINACTIVE>=SYSDATE
             AND U.UNIQUEITEM_ID=UF.UNIQUEITEM_ID
             AND U.UNIQUEITEM_ID=UN.UNIQUEITEM_ID
             AND UN.WEAREREMPLOYMENT_ID=WP.WEAREREMPLOYMENT_ID
             AND U.PRODUCT_ID=P.PRODUCT_ID
             AND P.PRODUCT_ID=PD.PRODUCT_ID
             AND PD.LANGUAGE_ID=1
             AND U.SIZEDEFINITION_ID=S.SIZEDEFINITION_ID
             AND W.WEARER_ID=WP.WEARER_ID
             AND W.CUSTOMER_ID=C.CUSTOMER_ID
             ORDER BY W.WEARERNUMBER,P.CODE,S.CODE")

rs <- dbSendQuery(conn, sql)
df <- fetch(rs)

#--查询UNIQUEITME_LINK_ID ②for unique --
sql2 = paste0("select uil.UNIQUEITEM_LINK_ID,ui.uniqueitem_id,ui.PRIMARYID,ui.WASHESTOTAL from abssolute.uniqueitem ui,abssolute.uniqueitem_link uil 
               where ui.uniqueitem_id = uil.uniqueitem_id
               and ui.PRIMARYID in(SELECT U.PRIMARYID
FROM ABSSOLUTE.UNIQUEITEM U,ABSSOLUTE.UNIQUEITEMFIXEDINFO UF,ABSSOLUTE.WEAREREMPLOYMENT WP,ABSSOLUTE.WEARER W,
             ABSSOLUTE.CUSTOMER C,ABSSOLUTE.UNIQUEITEMNONPOOL UN,ABSSOLUTE.STAY_DESC SD
             WHERE C.CUSTOMERNUMBER=",CUSTOMERNUMBER,"
             AND U.STATUS_ID=26
             AND U.STAY_ID=SD.STAY_ID
             AND SD.LANGUAGE_ID=1
             AND W.DATEINACTIVE>=SYSDATE
             AND U.UNIQUEITEM_ID=UF.UNIQUEITEM_ID
             AND U.UNIQUEITEM_ID=UN.UNIQUEITEM_ID
             AND UN.WEAREREMPLOYMENT_ID=WP.WEAREREMPLOYMENT_ID
             AND W.WEARER_ID=WP.WEARER_ID
             AND W.CUSTOMER_ID=C.CUSTOMER_ID
             )")              

rs2 <- dbSendQuery(conn, sql2)
data2 <- fetch(rs2)

#--查询Wash次数
sql31 = paste0("select s.uniqueitem_link_id,count(1) as WASHESTOTAL_2
               from abssolute.scan s,abssolute.transactiontype tt
               where s.transactiontype_id IN (1 , 4)
               and tt.transactiontype_id = s.transactiontype_id     
               and s.uniqueitem_link_id IN 
               (select uil.UNIQUEITEM_LINK_ID from abssolute.uniqueitem ui,abssolute.uniqueitem_link uil 
               where ui.uniqueitem_id = uil.uniqueitem_id
               and ui.PRIMARYID in(SELECT U.PRIMARYID
            FROM ABSSOLUTE.UNIQUEITEM U,ABSSOLUTE.UNIQUEITEMFIXEDINFO UF,ABSSOLUTE.WEAREREMPLOYMENT WP,ABSSOLUTE.WEARER W,
             ABSSOLUTE.CUSTOMER C,ABSSOLUTE.UNIQUEITEMNONPOOL UN,ABSSOLUTE.STAY_DESC SD
             WHERE C.CUSTOMERNUMBER=",CUSTOMERNUMBER,"
             AND U.STATUS_ID=26
             AND U.STAY_ID=SD.STAY_ID
             AND SD.LANGUAGE_ID=1
             AND W.DATEINACTIVE>=SYSDATE
             AND U.UNIQUEITEM_ID=UF.UNIQUEITEM_ID
             AND U.UNIQUEITEM_ID=UN.UNIQUEITEM_ID
             AND UN.WEAREREMPLOYMENT_ID=WP.WEAREREMPLOYMENT_ID
             AND W.WEARER_ID=WP.WEARER_ID
             AND W.CUSTOMER_ID=C.CUSTOMER_ID
            ))
               group by s.uniqueitem_link_id
               ")

rs31 <- dbSendQuery(conn, sql31)
data3 <- fetch(rs31)

#--关联PRIMARYID和WASH次数
result1 <- merge(data2, data3, all=TRUE)
result1[is.na(result1)] <- 0

diff <- filter(result1,result1$WASHESTOTAL != result1$WASHESTOTAL_2)
diff <- filter(diff,diff$WASHESTOTAL-diff$WASHESTOTAL_2 !=1)

#--关联人员和WASH次数
result <- merge(df, result1, by.x="PRIMARYID" , by.y= "PRIMARYID",all.x=TRUE)
dbDisconnect(conn)
#--写入文件
library(xlsx)
write.xlsx(x = diff, file = "20170629_10048_WT.xlsx",
           sheetName = "Sheet1", row.names = FALSE)


###---12. 查询衣服firstissue date  ---###
setwd("D:\\R\\cintas")
library(ROracle)
drv <- dbDriver("Oracle")
conn <- dbConnect(drv, username="REPORTING", password="REPORTING",
                  dbname="192.168.1.101:1521/ABSPR")
library(XLConnect)
library(reshape2)
library(sca)
library(plyr)

wb = loadWorkbook("10196 LR.xlsx")
df = readWorksheet(wb,sheet=1,header=TRUE)

sql = paste0("SELECT PRIMARYID,FIRSTISSUEDATE,ceil(DAYSINCIRCULATION/7) as weeks FROM ABSSOLUTE.RAGGEDITEM WHERE PRIMARYID IN
              (", paste0(shQuote(df[,1], 'sh'), collapse = ','), ")
             ")

rs <- dbSendQuery(conn, sql)
df1 <- fetch(rs)

sql2 = paste0("select a.primaryid,b.FIRSTISSUEDATE,b.DAYSINCIRCPREVISSUE as WEEKS from abssolute.uniqueitem a left join ABSSOLUTE.UNIQUEITEMFIXEDINFO b on a.UNIQUEITEM_ID = b.UNIQUEITEM_ID where a.PRIMARYID in
              (", paste0(shQuote(df[,1], 'sh'), collapse = ','), ")
              ")
rs <- dbSendQuery(conn, sql2)
df2 <- fetch(rs)

result <- rbind(df1,df2)
library(xlsx)
write.xlsx(x = result, file = "LR_FIRSTISSUEDATE_20171026.xlsx",
           sheetName = "Sheet1", row.names = FALSE)


#--关联人员
result <- merge(df, df1, by.x="PRIMARYID" , by.y= "PRIMARYID",all.x=TRUE)
result2 <- result[order(result$INVOICEDATE,result$WEARERNUMBER),]
dbDisconnect(conn)
#--写入文件
library(xlsx)
write.xlsx(x = result2, file = "20170808_10196_LR.xlsx",
           sheetName = "Sheet1", row.names = FALSE)


###---返回相差周数---###
today <- Sys.Date()

today <- as.Date("2016-10-25")

gtd <- as.Date("2016-5-6")   

today - gtd

#用difftime()函数可以计算相关的秒数、分钟数、小时数、天数、周数

difftime(today, gtd, units="weeks")  #还可以是“secs”, “mins”, “hours”, “days”


###--- ---###
###---13. 查询衣服repair 次数  ---###
setwd("D:\\R\\cintas")
library(ROracle)
drv <- dbDriver("Oracle")
conn <- dbConnect(drv, username="REPORTING", password="REPORTING",
                  dbname="192.168.1.101:1521/ABSPR")
library(XLConnect)
library(reshape2)
library(sca)
library(dplyr)
CUSTOMERNUMBER =10175

sql = paste0("SELECT C.CUSTOMERNUMBER,W.WEARERNUMBER,W.FULLNAME,W.DATEACTIVE,U.PRIMARYID,U.WASHESTOTAL,P.CODE,PD.DESCRIPTION,S.CODE AS SIZE_CODE,UF.FIRSTISSUEDATE,UF.LASTISSUEDATE,U.LASTOUTSCANDATE,U.LASTSCANDATE,SD.DESCRIPTION
             FROM ABSSOLUTE.UNIQUEITEM U,ABSSOLUTE.UNIQUEITEMFIXEDINFO UF,ABSSOLUTE.WEAREREMPLOYMENT WP,ABSSOLUTE.PRODUCT P,ABSSOLUTE.PRODUCT_DESC PD,ABSSOLUTE.SIZEDEFINITION S,ABSSOLUTE.WEARER W,
             ABSSOLUTE.CUSTOMER C,ABSSOLUTE.UNIQUEITEMNONPOOL UN,ABSSOLUTE.STAY_DESC SD
             WHERE C.CUSTOMERNUMBER=",CUSTOMERNUMBER,"
             AND U.STATUS_ID=26
             AND U.STAY_ID=SD.STAY_ID
             AND SD.LANGUAGE_ID=1
             AND W.DATEINACTIVE>=SYSDATE
             AND U.UNIQUEITEM_ID=UF.UNIQUEITEM_ID
             AND U.UNIQUEITEM_ID=UN.UNIQUEITEM_ID
             AND UN.WEAREREMPLOYMENT_ID=WP.WEAREREMPLOYMENT_ID
             AND U.PRODUCT_ID=P.PRODUCT_ID
             AND P.PRODUCT_ID=PD.PRODUCT_ID
             AND PD.LANGUAGE_ID=1
             AND U.SIZEDEFINITION_ID=S.SIZEDEFINITION_ID
             AND W.WEARER_ID=WP.WEARER_ID
             AND W.CUSTOMER_ID=C.CUSTOMER_ID
             ORDER BY W.WEARERNUMBER,P.CODE,S.CODE")

rs <- dbSendQuery(conn, sql)
df <- fetch(rs)

#--查询UNIQUEITME_LINK_ID ②for unique --
sql2 = paste0("select uil.UNIQUEITEM_LINK_ID,ui.uniqueitem_id,ui.PRIMARYID,ui.WASHESTOTAL
from abssolute.uniqueitem ui,abssolute.uniqueitem_link uil,abssolute.uniqueitemnonpool u
              where ui.uniqueitem_id = uil.uniqueitem_id
              and uil.UNIQUEITEM_ID = u.UNIQUEITEM_ID
              and ui.PRIMARYID in(SELECT U.PRIMARYID
              FROM ABSSOLUTE.UNIQUEITEM U,ABSSOLUTE.UNIQUEITEMFIXEDINFO UF,ABSSOLUTE.WEAREREMPLOYMENT WP,ABSSOLUTE.WEARER W,
              ABSSOLUTE.CUSTOMER C,ABSSOLUTE.UNIQUEITEMNONPOOL UN,ABSSOLUTE.STAY_DESC SD
              WHERE C.CUSTOMERNUMBER=",CUSTOMERNUMBER,"
              AND U.STATUS_ID=26
              AND U.STAY_ID=SD.STAY_ID
              AND SD.LANGUAGE_ID=1
              AND W.DATEINACTIVE>=SYSDATE
              AND U.UNIQUEITEM_ID=UF.UNIQUEITEM_ID
              AND U.UNIQUEITEM_ID=UN.UNIQUEITEM_ID
              AND UN.WEAREREMPLOYMENT_ID=WP.WEAREREMPLOYMENT_ID
              AND W.WEARER_ID=WP.WEARER_ID
              AND W.CUSTOMER_ID=C.CUSTOMER_ID
              )")              

rs2 <- dbSendQuery(conn, sql2)
data2 <- fetch(rs2)

#--查询REPAIR次数
sql31 = paste0("select s.uniqueitem_link_id,count(1) as WASHTOTAL_2
               from abssolute.scan s,abssolute.transactiontype tt
               where s.transactiontype_id IN (1,4)
               and tt.transactiontype_id = s.transactiontype_id     
               and s.uniqueitem_link_id IN 
               (select uil.UNIQUEITEM_LINK_ID from abssolute.uniqueitem ui,abssolute.uniqueitem_link uil 
               where ui.uniqueitem_id = uil.uniqueitem_id
               and ui.PRIMARYID in(SELECT U.PRIMARYID
               FROM ABSSOLUTE.UNIQUEITEM U,ABSSOLUTE.UNIQUEITEMFIXEDINFO UF,ABSSOLUTE.WEAREREMPLOYMENT WP,ABSSOLUTE.WEARER W,
               ABSSOLUTE.CUSTOMER C,ABSSOLUTE.UNIQUEITEMNONPOOL UN,ABSSOLUTE.STAY_DESC SD
               WHERE C.CUSTOMERNUMBER=",CUSTOMERNUMBER,"
               AND U.STATUS_ID=26
               AND U.STAY_ID=SD.STAY_ID
               AND SD.LANGUAGE_ID=1
               AND W.DATEINACTIVE>=SYSDATE
               AND U.UNIQUEITEM_ID=UF.UNIQUEITEM_ID
               AND U.UNIQUEITEM_ID=UN.UNIQUEITEM_ID
               AND UN.WEAREREMPLOYMENT_ID=WP.WEAREREMPLOYMENT_ID
               AND W.WEARER_ID=WP.WEARER_ID
               AND W.CUSTOMER_ID=C.CUSTOMER_ID
               ))
               group by s.uniqueitem_link_id
               ")

rs31 <- dbSendQuery(conn, sql31)
data3 <- fetch(rs31)

#--关联PRIMARYID和WASH次数
result1 <- merge(data2, data3, all=TRUE)
result1[is.na(result1)] <- 0

diff <- filter(result1,result1$WASHESTOTAL != result1$WASHTOTAL_2)
diff <- filter(diff,diff$WASHTOTAL_2-diff$WASHESTOTAL != 1)

#--关联人员和WASH次数
result <- merge(df, result1, by.x="PRIMARYID" , by.y= "PRIMARYID",all.x=TRUE)
dbDisconnect(conn)
#--写入文件
library(xlsx)
write.xlsx(x = diff, file = "20170629_10048_WT.xlsx",
           sheetName = "Sheet1", row.names = FALSE)



###---14. 需回库衣服---###
setwd("D:\\R\\cintas")
library(ROracle)
drv <- dbDriver("Oracle")
conn <- dbConnect(drv, username="CINTASBI", password="CINTASBI2015",
                  dbname="192.168.1.230:1521/CINTASBI")
library(XLConnect)
library(reshape2)
library(sca)
library(dplyr)
CUSTOMERNUMBER =10000

#--查询UNIQUEITME_LINK_ID ②for unique --
sql = paste0("SELECT * FROM V_QUITWEARERGARMENTS WHERE CUSTOMERNUMBER =",CUSTOMERNUMBER,"
              UNION
              SELECT * FROM ABSSOLUTE.V_TOBERETURNEDGARMENTS_REP_G@TO_ABS1 WHERE CUSTOMERNUMBER=",CUSTOMERNUMBER,"")              

rs <- dbSendQuery(conn, sql)
NR <- fetch(rs)

#--查询UNIQUEITME_LINK_ID ②for unique --

conn <- dbConnect(drv, username="REPORTING", password="REPORTING",
                  dbname="192.168.1.101:1521/ABSPR")
sql2 = paste0("select uil.UNIQUEITEM_LINK_ID,ui.uniqueitem_id,ui.PRIMARYID,ui.WASHESTOTAL 
              from abssolute.uniqueitem ui,abssolute.uniqueitem_link uil,abssolute.uniqueitemnonpool u
              where ui.uniqueitem_id = uil.uniqueitem_id
              and uil.UNIQUEITEM_ID = u.UNIQUEITEM_ID
              and ui.PRIMARYID in
               (", paste0(shQuote(NR[,8], 'sh'), collapse = ','), ")
              ")              

rs2 <- dbSendQuery(conn, sql2)
data2 <- fetch(rs2)

#--查询REPAIR次数
sql31 = paste0("select s.uniqueitem_link_id,count(1) as WASHTOTAL_2
               from abssolute.scan s,abssolute.transactiontype tt
               where s.transactiontype_id IN (1,4)
               and tt.transactiontype_id = s.transactiontype_id     
               and s.uniqueitem_link_id IN 
               (select uil.UNIQUEITEM_LINK_ID from abssolute.uniqueitem ui,abssolute.uniqueitem_link uil 
               where ui.uniqueitem_id = uil.uniqueitem_id
               and ui.PRIMARYID in
                (", paste0(shQuote(NR[,8], 'sh'), collapse = ','), ")
                )
               group by s.uniqueitem_link_id
               ")

rs31 <- dbSendQuery(conn, sql31)
data3 <- fetch(rs31)

#--关联PRIMARYID和WASH次数
result1 <- merge(data2, data3, all=TRUE)
result1[is.na(result1)] <- 0

diff <- filter(result1,result1$WASHESTOTAL != result1$WASHTOTAL_2)
diff <- filter(diff,diff$WASHESTOTAL-diff$WASHESTOTAL_2 !=1)


###-----15. 
setwd("D:\\R\\cintas")
library(ROracle)
drv <- dbDriver("Oracle")
conn <- dbConnect(drv, username="CINTASBI", password="CINTASBI2015",
                  dbname="192.168.1.230:1521/CINTASBI")
library(XLConnect)
library(reshape2)
library(sca)
library(dplyr)
library(sqldf)
CUSTOMERNUMBER =10175

#--查询UNIQUEITME_LINK_ID ②for unique --
sql = paste0("SELECT TO_CHAR(C.CUSTOMERNUMBER) CUSTOMERNUMBER,C.NAME,TO_CHAR(W.WEARERNUMBER) WEARERNUMBER,W.FULLNAME,W.CUSTOMEREMPLOYEENUMBER, W.DATEINACTIVE STOPDATE,H.BANKLOCKER,
              UI.PRIMARYID, P.CODE PRODUCT,PRI.PRICE, S.CODE SIZECODE, PD.DESCRIPTION,UI.CUSTOMEROWNED,(4-(GETISOWEEK@TO_ABS1(SYSDATE)+(YEAR(SYSDATE)-YEAR(W.DATEINACTIVE))*52 - GETISOWEEK@TO_ABS1(W.DATEINACTIVE))) || ' WEEKS LEFT' REMAINS,'QUIT WEARER' REMARK,'SA' TYPE,
             ROUTE.DESCRIPTION ROUTE,C.DEFAULTBUSINESSUNIT_ID,C.CUSTOMER_ID
             FROM WEARER@TO_ABS1 W, WEAREREMPLOYMENT@TO_ABS1 WE, WEARERINVENTORY@TO_ABS1 WI, UNIQUEITEMNONPOOL@TO_ABS1 UINP, UNIQUEITEM@TO_ABS1 UI, PRODUCT@TO_ABS1 P, SIZEDEFINITION@TO_ABS1 S,
             PRODUCT_DESC@TO_ABS1 PD,CUSTOMER@TO_ABS1 C,V_REPPRICE_G@TO_ABS1 PRI, STAY@TO_ABS1 ST, V_CMFHEADER H, (SELECT R.DESCRIPTION,RT.CUSTOMER_ID FROM ROUTE@TO_ABS1 R, ROUTESTOP@TO_ABS1 RT WHERE R.ROUTE_ID=RT.ROUTE_ID AND RT.DATEINACTIVE>SYSDATE
             GROUP BY R.DESCRIPTION,RT.CUSTOMER_ID ORDER BY CUSTOMER_ID) ROUTE
             WHERE 
             W.WEARER_ID=WE.WEARER_ID
             AND WE.WEAREREMPLOYMENT_ID = WI.WEAREREMPLOYMENT_ID
             AND WE.WEAREREMPLOYMENT_ID=UINP.WEAREREMPLOYMENT_ID
             AND WI.WEARERINVENTORY_ID=UINP.WEARERINVENTORY_ID
             AND UINP.UNIQUEITEM_ID=UI.UNIQUEITEM_ID
             AND WI.PRODUCT_ID=P.PRODUCT_ID
             AND WI.SIZEDEFINITION_ID = S.SIZEDEFINITION_ID
             AND P.PRODUCT_ID=PD.PRODUCT_ID
             AND UI.PRODUCT_ID=PRI.PRODUCT_ID
             AND W.CUSTOMER_ID = PRI.CUSTOMER_ID
             AND C.CUSTOMER_ID=H.CUSTOMER_ID AND W.WEARER_ID=H.WEARER_ID
             AND PD.LANGUAGE_ID=1
             AND W.CUSTOMER_ID=C.CUSTOMER_ID
             AND UI.STAY_ID=ST.STAY_ID
             AND C.CUSTOMER_ID=ROUTE.CUSTOMER_ID
             AND UI.STATUS_ID=26 AND UI.STAY_ID IN (3,18,26)
             AND W.DATEINACTIVE>=SYSDATE-35 AND W.DATEINACTIVE<=SYSDATE
             AND C.CUSTOMERNUMBER = ",CUSTOMERNUMBER,"
             ")              

rs <- dbSendQuery(conn, sql)
NR <- fetch(rs)

PN <- read.csv2("PN.txt",header= T,  sep=",", encoding = "UTF-8")

result <- sqldf("select * from NR where PRODUCT in (select PN from PN)")


result <-  filter(result,CUSTOMEROWNED == 'N')

sqldf("select distinct CUSTOMERNUMBER,name from result")

result %>%
  filter(CUSTOMERNUMBER == '10033') %>%
  select(WEARERNUMBER,PRIMARYID,PRODUCT,REMAINS)

NR %>%
  filter(CUSTOMERNUMBER == '10033') %>%
  select(WEARERNUMBER,PRIMARYID,PRODUCT,REMAINS)


library(xlsx)
write.xlsx(x = result, file = "10033_non_COG.xlsx",
           sheetName = "Sheet1", row.names = FALSE)

NR %>%
  filter(CUSTOMEROWNED == 'N',PRODUCT == '897451200',WEARERNUMBER == '8302') %>%
  select(WEARERNUMBER,PRIMARYID,PRODUCT,REMAINS)


###---16. 查询衣服firstissue date, PN  ---###
setwd("D:\\R\\cintas")
library(ROracle)
drv <- dbDriver("Oracle")
conn <- dbConnect(drv, username="REPORTING", password="REPORTING",
                  dbname="192.168.1.101:1521/ABSPR")
library(XLConnect)
library(reshape2)
library(sca)
library(plyr)
library(sqldf)

#SN <- read.csv2("ID.txt",header= T,  sep=",")
wb = loadWorkbook("20170927.xlsx")
SN = readWorksheet(wb,sheet=1,header=TRUE)

# unRaged 
sql = paste0("SELECT U.PRIMARYID,UI.FIRSTISSUEDATE,CEIL((TRUNC(SYSDATE)-TRUNC(UI.FIRSTISSUEDATE)-UI.DAYSINCIRCPREVISSUE)/7) AS WEEKS,P.CODE,PD.DESCRIPTION
FROM ABSSOLUTE.UNIQUEITEM U,ABSSOLUTE.UNIQUEITEMFIXEDINFO UI,ABSSOLUTE.PRODUCT P,ABSSOLUTE.PRODUCT_DESC PD
             WHERE U.UNIQUEITEM_ID = UI.UNIQUEITEM_ID
             AND U.PRODUCT_ID = P.PRODUCT_ID
             AND U.PRODUCT_ID = PD.PRODUCT_ID
             AND PD.LANGUAGE_ID =1
             AND U.PRIMARYID IN (", paste0(shQuote(SN[,1], 'sh'), collapse = ','), ")
             ")

rs <- dbSendQuery(conn, sql)
df1 <- fetch(rs)

#-- Raged 
SN2 <- sqldf("SELECT * FROM SN WHERE PRIMARYID NOT IN (SELECT PRIMARYID FROM df1)")
sql = paste0("SELECT PRIMARYID,FIRSTISSUEDATE,CEIL(DAYSINCIRCULATION/7) AS WEEKS,P.CODE,PD.DESCRIPTION
              FROM ABSSOLUTE.RAGGEDITEM R,ABSSOLUTE.PRODUCT_LINK P,ABSSOLUTE.PRODUCT_LINK_DESC PD 
              WHERE R.PRODUCT_LINK_ID = P.PRODUCT_LINK_ID
              AND P.PRODUCT_LINK_ID = PD.PRODUCT_LINK_ID
              AND PD.LANGUAGE_ID =1
              AND PRIMARYID IN (", paste0(shQuote(SN2[,1], 'sh'), collapse = ','), ")
             ")

rs <- dbSendQuery(conn, sql)
df2 <- fetch(rs)

df <- rbind(df1,df2)
result <- merge(SN, df, by.x="PRIMARYID" , by.y= "PRIMARYID",all.x=TRUE)
dbDisconnect(conn)

#--写入文件
library(xlsx)
write.xlsx(x = result, file = "20170929_10196_ID.xlsx",
           sheetName = "Sheet1", row.names = FALSE)

##-----
setwd("D://R//cintas//LR_Wash")
library(ROracle)
library(sqldf)
library(dplyr)
library(reshape2)
library(sca)
drv <- dbDriver("Oracle")
conn <- dbConnect(drv, username="REPORTING", password="REPORTING",
                  dbname="192.168.1.101:1521/ABSPR",DBMSencoding="utf8")

sql = paste0("select b.code,a.language_id,a.description  from abssolute.flag_desc a,abssolute.flag b where a.flag_id = b.flag_id")

rs <- dbSendQuery(conn, sql)
df1 <- fetch(rs)
result = dcast(df1,CODE~LANGUAGE_ID)
#--写入文件
library(xlsx)
write.xlsx(x = result, file = "flag.xlsx",
           sheetName = "Sheet1", row.names = FALSE)
