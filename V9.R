###---3 查询Wash Total---###
setwd("D://R//cintas//LR_Wash")
library(ROracle)
drv <- dbDriver("Oracle")
conn <- dbConnect(drv, username="ABSSOLUTE", password="ABSSOLUTE",
                  dbname="192.168.1.104:1521/ABST",DBMSencoding="utf8")

CUSTOMERNUMBER ="10119"
INVOICEDATE1 ="2017-7-1"
INVOICEDATE2 ="2017-10-24"
FILENAME = paste0(getwd(),'/export/',CUSTOMERNUMBER,'_LR.xlsx')
#--查询 PrimaryID
sql1 = paste0("SELECT Z.*,SL.CODE FROM(
              SELECT BU.DESCRIPTION as LOCATION,IH.BILLTOCUSTOMERNUMBER,IH.BILLTOCUSTOMERNAME,IH.INVOICEDATE,IH.INVOICENUMBER,OH.PRIMARYID,IP.WEARERNUMBER,IP.WEARERNAME,IL.DESCRIPTION, IL.CODE PRODUCT,OH.SIZEDEFINITION_LINK_ID
              FROM abssolute.INVOICELINE IL
              LEFT JOIN abssolute.INVOICEHEADER IH ON IH.INVOICEHEADER_ID = IL.INVOICEHEADER_ID
              LEFT JOIN abssolute.INVOICELINETYPE ILT ON ILT.INVOICELINETYPE_ID = IL.INVOICELINETYPE_ID
              LEFT JOIN abssolute.BUSINESSUNIT BU ON BU.BUSINESSUNIT_ID = IL.PROCESSINGBUSINESSUNIT_ID
              LEFT JOIN abssolute.INVOICELINEPRODUCT IP ON IP.INVOICELINE_ID = IL.INVOICELINE_ID
              LEFT JOIN abssolute.OUTOFCIRCULATIONHISTORY OH ON OH.CUSTOMER_LINK_ID = IH.BILLTOCUSTOMER_LINK_ID
              --AND OH.OUTOFCIRCYEAR = TO_NUMBER(TO_CHAR(SYSDATE, 'yyyy'))
              AND OH.WEAREREMPLOYMENT_LINK_ID = IP.WEAREREMPLOYMENT_LINK_ID
              AND OH.PRODUCT_LINK_ID = IP.PRODUCT_LINK_ID
              AND OH.SIZEDEFINITION_LINK_ID = IP.SIZEDEFINITION_LINK_ID
              AND OH.TIMESTAMP > IH.INVOICEDATE
              WHERE IL.INVOICELINETYPE_ID = 9
              AND IL.EXTEND = 'Y'
              AND IL.AMOUNT > 0
              AND OH.REASONCODE_ID <> 7
              AND IH.INVOICEDATE >= TO_DATE('",INVOICEDATE1,"', 'yyyy-mm-dd')
              AND IH.INVOICEDATE <= TO_DATE('",INVOICEDATE2,"', 'yyyy-mm-dd')
              AND IH.BILLTOCUSTOMERNUMBER = '",CUSTOMERNUMBER,"'
              AND BU.CODE = 1
              ORDER BY IL.INVOICELINETYPE_ID, IP.WEARERNUMBER ) Z LEFT JOIN abssolute.SIZEDEFINITION_LINK SL ON Z.SIZEDEFINITION_LINK_ID = SL.SIZEDEFINITION_LINK_ID")

rs1 <- dbSendQuery(conn, sql1)
data1 <- fetch(rs1)
data1 <- unique(data1)
dbDisconnect(conn)
conn <- dbConnect(drv, username="ABSSOLUTE", password="ABSSOLUTE",
                  dbname="192.168.1.104:1521/ABST")

#--查询UNIQUEITME_LINK_ID ①for Ragged --
sql21 = paste0("select uil.UNIQUEITEM_LINK_ID,ri.RAGGEDITEM_ID,ri.PRIMARYID,ri.WASHESTOTAL from abssolute.raggeditem ri,abssolute.uniqueitem_link uil 
               where ri.raggeditem_id = uil.raggeditem_id
               and ri.PRIMARYID in(",paste0(shQuote(data1[1:1000,6], 'sh'), collapse = ','),")")

#--查询UNIQUEITME_LINK_ID ②for unique --
sql22 = paste0("select uil.UNIQUEITEM_LINK_ID,ui.uniqueitem_id,ui.PRIMARYID,ui.WASHESTOTAL from abssolute.uniqueitem ui,abssolute.uniqueitem_link uil 
               where ui.uniqueitem_id = uil.uniqueitem_id
               and ui.PRIMARYID in(",paste0(shQuote(data1[1:1000,6], 'sh'), collapse = ','),")")              

rs21 <- dbSendQuery(conn, sql21)
data21 <- fetch(rs21)
data2 <- rbind(data2,data21[,-2])
rs22 <- dbSendQuery(conn, sql22)
data22 <- fetch(rs22)
data2 <- rbind(data2,data22[,-2])

#--合并UNIQUEITME_LINK_ID数据 --
data2 <- rbind(data21[,-2],data22[,-2])

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
result <- merge(data1, result, by.x="PRIMARYID",by.y="PRIMARYID",all.y=TRUE)
result <- result[order(result$WEARERNUMBER),]

#--写入文件
write.csv(result,file="V9_Test.csv",row.names=FALSE)
dbDisconnect(conn)

library(xlsx)
write.xlsx(x = data1, file = FILENAME, sheetName = "Sheet1", row.names = FALSE)
dbDisconnect(conn)
