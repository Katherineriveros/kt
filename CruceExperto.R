library(sparklyr)
library(DBI)
library(dplyr)
library(psych)
library(tidyverse)
library(data.table)
library(lubridate)
setwd("/shared/nas/shape/bu_co/persist/darriet")


#Importo la base a tratar y le asigno nombre
BaseCastigo<-fread("BASE_CAL_CASTIGO_.txt")
names(BaseCastigo)<-tolower(names(BaseCastigo))

# basic spark conncection
sc <- spark_connect(master = "yarn-client")


# connect to core DB - establece conexión con tablas de Colombia


dbGetQuery(sc, "use colombia")

# Se fracciona la base importada en meses para luego hacer el pegue de variables de experto
m201402 <- BaseCastigo%>% filter (fecha_calificacion == '01Feb2014')
m201403 <- BaseCastigo%>% filter (fecha_calificacion == '01Mar2014')
m201404 <- BaseCastigo%>% filter (fecha_calificacion == '01Apr2014')
m201405 <- BaseCastigo%>% filter (fecha_calificacion == '01May2014')
m201406 <- BaseCastigo%>% filter (fecha_calificacion == '01Jun2014')
m201407 <- BaseCastigo%>% filter (fecha_calificacion == '01Jul2014')
m201408 <- BaseCastigo%>% filter (fecha_calificacion == '01Aug2014')
m201409 <- BaseCastigo%>% filter (fecha_calificacion == '01Sep2014')
m201410 <- BaseCastigo%>% filter (fecha_calificacion == '01Oct2014')
m201411 <- BaseCastigo%>% filter (fecha_calificacion == '01Nov2014')
m201412 <- BaseCastigo%>% filter (fecha_calificacion == '01Dec2014')

m201501 <- BaseCastigo%>% filter (fecha_calificacion == '01Jan2015')
m201502 <- BaseCastigo%>% filter (fecha_calificacion == '01Feb2015')
m201503 <- BaseCastigo%>% filter (fecha_calificacion == '01Mar2015')
m201504 <- BaseCastigo%>% filter (fecha_calificacion == '01Apr2015')
m201505 <- BaseCastigo%>% filter (fecha_calificacion == '01May2015')
m201506 <- BaseCastigo%>% filter (fecha_calificacion == '01Jun2015')
m201507 <- BaseCastigo%>% filter (fecha_calificacion == '01Jul2015')
m201508 <- BaseCastigo%>% filter (fecha_calificacion == '01Aug2015')
m201509 <- BaseCastigo%>% filter (fecha_calificacion == '01Sep2015')
m201510 <- BaseCastigo%>% filter (fecha_calificacion == '01Oct2015')
m201511 <- BaseCastigo%>% filter (fecha_calificacion == '01Nov2015')
m201512 <- BaseCastigo%>% filter (fecha_calificacion == '01Dec2015')

m201601 <- BaseCastigo%>% filter (fecha_calificacion == '01Jan2016')
m201602 <- BaseCastigo%>% filter (fecha_calificacion == '01Feb2016')
m201603 <- BaseCastigo%>% filter (fecha_calificacion == '01Mar2016')
m201604 <- BaseCastigo%>% filter (fecha_calificacion == '01Apr2016')
m201605 <- BaseCastigo%>% filter (fecha_calificacion == '01May2016')
m201606 <- BaseCastigo%>% filter (fecha_calificacion == '01Jun2016')
m201607 <- BaseCastigo%>% filter (fecha_calificacion == '01Jul2016')
m201608 <- BaseCastigo%>% filter (fecha_calificacion == '01Aug2016')
m201609 <- BaseCastigo%>% filter (fecha_calificacion == '01Sep2016')
m201610 <- BaseCastigo%>% filter (fecha_calificacion == '01Oct2016')
m201611 <- BaseCastigo%>% filter (fecha_calificacion == '01Nov2016')
m201612 <- BaseCastigo%>% filter (fecha_calificacion == '01Dec2016')

m201701 <- BaseCastigo%>% filter (fecha_calificacion == '01Jan2017')
m201702 <- BaseCastigo%>% filter (fecha_calificacion == '01Feb2017')
m201704 <- BaseCastigo%>% filter (fecha_calificacion == '01Apr2017')

summary(BaseCastigo$fecha_calificacion)


# connect lazily to a big table
experto <- tbl(sc, "co_base_experto")




# filtros de los expertos por secuencia tercero y las variables a traer
experto_sample<-experto%>%filter(data_date =='2014-02-28')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201403 <-experto%>%filter(data_date =='2014-03-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201404 <-experto%>%filter(data_date =='2014-04-30')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201405 <-experto%>%filter(data_date =='2014-05-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201406 <-experto%>%filter(data_date =='2014-06-30')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201407 <-experto%>%filter(data_date =='2014-07-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201408 <-experto%>%filter(data_date =='2014-08-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201409 <-experto%>%filter(data_date =='2014-09-30')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201410 <-experto%>%filter(data_date =='2014-10-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201411 <-experto%>%filter(data_date =='2014-11-30')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201412 <-experto%>%filter(data_date =='2014-12-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)

experto_201501 <-experto%>%filter(data_date =='2015-01-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201502 <-experto%>%filter(data_date =='2015-02-28')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201503 <-experto%>%filter(data_date =='2015-03-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201504 <-experto%>%filter(data_date =='2015-04-30')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201505 <-experto%>%filter(data_date =='2015-05-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201506 <-experto%>%filter(data_date =='2015-06-30')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201507 <-experto%>%filter(data_date =='2015-07-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201508 <-experto%>%filter(data_date =='2015-08-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201509 <-experto%>%filter(data_date =='2015-09-30')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201510 <-experto%>%filter(data_date =='2015-10-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201511 <-experto%>%filter(data_date =='2015-11-30')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201512 <-experto%>%filter(data_date =='2015-12-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)

experto_201601 <-experto%>%filter(data_date =='2016-01-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201602 <-experto%>%filter(data_date =='2016-02-29')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201603 <-experto%>%filter(data_date =='2016-03-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201604 <-experto%>%filter(data_date =='2016-04-30')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201605 <-experto%>%filter(data_date =='2016-05-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201606 <-experto%>%filter(data_date =='2016-06-30')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201607 <-experto%>%filter(data_date =='2016-07-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201608 <-experto%>%filter(data_date =='2016-08-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201609 <-experto%>%filter(data_date =='2016-09-30')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201610 <-experto%>%filter(data_date =='2016-10-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201611 <-experto%>%filter(data_date =='2016-11-30')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201612 <-experto%>%filter(data_date =='2016-12-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)

experto_201701 <-experto%>%filter(data_date =='2017-01-31')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201702 <-experto%>%filter(data_date =='2017-02-29')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)
experto_201704 <-experto%>%filter(data_date =='2017-04-30')%>%select(secuencia_tercero,tc_014,ct_025,tc_172,tc_131,ct_248,ct_261,con_001,con_002,con_003,sf_025,to_009)



#base mensuales cargadas en spark
m201402_1 <- copy_to(sc,m201402,"m201402_1")
m201403_1 <- copy_to(sc,m201403,"m201403_1")
m201404_1 <- copy_to(sc,m201404,"m201404_1")
m201405_1 <- copy_to(sc,m201405,"m201405_1")
m201406_1 <- copy_to(sc,m201406,"m201406_1")
m201407_1 <- copy_to(sc,m201407,"m201407_1")
m201408_1 <- copy_to(sc,m201408,"m201408_1")
m201409_1 <- copy_to(sc,m201409,"m201409_1")
m201410_1 <- copy_to(sc,m201410,"m201410_1")
m201411_1 <- copy_to(sc,m201411,"m201411_1")
m201412_1 <- copy_to(sc,m201412,"m201412_1")

m201501_1 <- copy_to(sc,m201501,"m201501_1")
m201502_1 <- copy_to(sc,m201502,"m201502_1")
m201503_1 <- copy_to(sc,m201503,"m201503_1")
m201504_1 <- copy_to(sc,m201504,"m201504_1")
m201505_1 <- copy_to(sc,m201505,"m201505_1")
m201506_1 <- copy_to(sc,m201506,"m201506_1")
m201507_1 <- copy_to(sc,m201507,"m201507_1")
m201508_1 <- copy_to(sc,m201508,"m201508_1")
m201509_1 <- copy_to(sc,m201509,"m201509_1")
m201510_1 <- copy_to(sc,m201510,"m201510_1")
m201511_1 <- copy_to(sc,m201511,"m201511_1")
m201512_1 <- copy_to(sc,m201512,"m201512_1")

m201601_1 <- copy_to(sc,m201601,"m201601_1")
m201602_1 <- copy_to(sc,m201602,"m201602_1")
m201603_1 <- copy_to(sc,m201603,"m201603_1")
m201604_1 <- copy_to(sc,m201604,"m201604_1")
m201605_1 <- copy_to(sc,m201605,"m201605_1")
m201606_1 <- copy_to(sc,m201606,"m201606_1")
m201607_1 <- copy_to(sc,m201607,"m201607_1")
m201608_1 <- copy_to(sc,m201608,"m201608_1")
m201609_1 <- copy_to(sc,m201609,"m201609_1")
m201610_1 <- copy_to(sc,m201610,"m201610_1")
m201611_1 <- copy_to(sc,m201611,"m201611_1")
m201612_1 <- copy_to(sc,m201612,"m201612_1")

m201701_1 <- copy_to(sc,m201701,"m201701_1")
m201702_1 <- copy_to(sc,m201702,"m201702_1")
m201704_1 <- copy_to(sc,m201704,"m201704_1")


# join de los bases mensuales de castigo con los expertos
Mes_feb2014 <-left_join(m201402_1,experto_sample)
Mes_Mar2014 <-left_join(m201403_1,experto_201403)
Mes_Abr2014 <-left_join(m201404_1,experto_201404)
Mes_May2014 <-left_join(m201405_1,experto_201405)
Mes_Jun2014 <-left_join(m201406_1,experto_201406)
Mes_Jul2014 <-left_join(m201407_1,experto_201407)
Mes_Ago2014 <-left_join(m201408_1,experto_201408)
Mes_Sep2014 <-left_join(m201409_1,experto_201409)
Mes_Oct2014 <-left_join(m201410_1,experto_201410)
Mes_Nov2014 <-left_join(m201411_1,experto_201411)
Mes_Dic2014 <-left_join(m201412_1,experto_201412)

Mes_Ene2015 <-left_join(m201501_1,experto_201501)
Mes_feb2015 <-left_join(m201502_1,experto_201502)
Mes_Mar2015 <-left_join(m201503_1,experto_201503)
Mes_Abr2015 <-left_join(m201504_1,experto_201504)
Mes_May2015 <-left_join(m201505_1,experto_201505)
Mes_Jun2015 <-left_join(m201506_1,experto_201506)
Mes_Jul2015 <-left_join(m201507_1,experto_201507)
Mes_Ago2015 <-left_join(m201508_1,experto_201508)
Mes_Sep2015 <-left_join(m201509_1,experto_201509)
Mes_Oct2015 <-left_join(m201510_1,experto_201510)
Mes_Nov2015 <-left_join(m201511_1,experto_201511)
Mes_Dic2015 <-left_join(m201512_1,experto_201512)

Mes_Ene2016 <-left_join(m201601_1,experto_201601)
Mes_feb2016 <-left_join(m201602_1,experto_201602)
Mes_Mar2016 <-left_join(m201603_1,experto_201603)
Mes_Abr2016 <-left_join(m201604_1,experto_201604)
Mes_May2016 <-left_join(m201605_1,experto_201605)
Mes_Jun2016 <-left_join(m201606_1,experto_201606)
Mes_Jul2016 <-left_join(m201607_1,experto_201607)
Mes_Ago2016 <-left_join(m201608_1,experto_201608)
Mes_Sep2016 <-left_join(m201609_1,experto_201609)
Mes_Oct2016 <-left_join(m201610_1,experto_201610)
Mes_Nov2016 <-left_join(m201611_1,experto_201611)
Mes_Dic2016 <-left_join(m201612_1,experto_201612)

Mes_Ene2017 <-left_join(m201701_1,experto_201701)
Mes_feb2017 <-left_join(m201702_1,experto_201702)
Mes_Abr2017 <-left_join(m201704_1,experto_201704)

#Muestra las Bases en R
Mes_feb2014<-Mes_feb2014%>%collect
Mes_Mar2014<-Mes_Mar2014%>%collect
Mes_Abr2014<-Mes_Abr2014%>%collect
Mes_May2014<-Mes_May2014%>%collect
Mes_Jun2014<-Mes_Jun2014%>%collect
Mes_Jul2014<-Mes_Jul2014%>%collect
Mes_Ago2014<-Mes_Ago2014%>%collect
Mes_Sep2014<-Mes_Sep2014%>%collect
Mes_Oct2014<-Mes_Oct2014%>%collect
Mes_Nov2014<-Mes_Nov2014%>%collect
Mes_Dic2014<-Mes_Dic2014%>%collect

Mes_Ene2015<-Mes_Ene2015%>%collect
Mes_feb2015<-Mes_feb2015%>%collect
Mes_Mar2015<-Mes_Mar2015%>%collect
Mes_Abr2015<-Mes_Abr2015%>%collect
Mes_May2015<-Mes_May2015%>%collect
Mes_Jun2015<-Mes_Jun2015%>%collect
Mes_Jul2015<-Mes_Jul2015%>%collect
Mes_Ago2015<-Mes_Ago2015%>%collect
Mes_Sep2015<-Mes_Sep2015%>%collect
Mes_Oct2015<-Mes_Oct2015%>%collect
Mes_Nov2015<-Mes_Nov2015%>%collect
Mes_Dic2015<-Mes_Dic2015%>%collect

Mes_Ene2016<-Mes_Ene2016%>%collect
Mes_feb2016<-Mes_feb2016%>%collect
Mes_Mar2016<-Mes_Mar2016%>%collect
Mes_Abr2016<-Mes_Abr2016%>%collect
Mes_May2016<-Mes_May2016%>%collect
Mes_Jun2016<-Mes_Jun2016%>%collect
Mes_Jul2016<-Mes_Jul2016%>%collect
Mes_Ago2016<-Mes_Ago2016%>%collect
Mes_Sep2016<-Mes_Sep2016%>%collect
Mes_Oct2016<-Mes_Oct2016%>%collect
Mes_Nov2016<-Mes_Nov2016%>%collect
Mes_Dic2016<-Mes_Dic2016%>%collect

Mes_Ene2017<-Mes_Ene2017%>%collect
Mes_feb2017<-Mes_feb2017%>%collect
Mes_Abr2017<-Mes_Abr2017%>%collect

#Union de los meses fraccionados para que la base quede del mismo tamaño que la inicial
TablaTotal <- rbind(Mes_feb2014,Mes_Mar2014,Mes_Abr2014,Mes_May2014,Mes_Jun2014,Mes_Jul2014,Mes_Ago2014,Mes_Sep2014,Mes_Oct2014,
                    Mes_Nov2014,Mes_Dic2014,Mes_Ene2015,Mes_feb2015,Mes_Mar2015,Mes_Abr2015,Mes_May2015,Mes_Jun2015,Mes_Jul2015,
                    Mes_Ago2015,Mes_Sep2015,Mes_Oct2015,Mes_Nov2015,Mes_Dic2015,Mes_Ene2016,Mes_feb2016,Mes_Mar2016,Mes_Abr2016,
                    Mes_May2016,Mes_Jun2016,Mes_Jul2016,Mes_Ago2016,Mes_Sep2016,Mes_Oct2016,Mes_Nov2016,Mes_Dic2016,Mes_Ene2017,
                    Mes_feb2017,Mes_Abr2017)

getwd() # espacio de trabajo donde estoy ubicado

# Guarda la tabla en el espacio de trabajo
write.csv(TablaTotal, 'Tabla_Total-Castigo.csv')


