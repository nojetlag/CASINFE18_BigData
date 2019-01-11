# Databricks notebook source
# MAGIC %md # Leistungsnachweis 
# MAGIC # Modul D Big Data
# MAGIC 
# MAGIC Dozent: Prof. Dr. Kurt Stockinger
# MAGIC 
# MAGIC 
# MAGIC Autoren: Andreas Palm und Reto Stucki
# MAGIC 
# MAGIC 
# MAGIC "11. Januar 2019"

# COMMAND ----------

# MAGIC %md
# MAGIC Basierend auf einem öffentlich erhältlichen Datensatz über Flugzeugabstürze(1908-2018) in dem zu jedem Flugzeugabsturz eine Kurzbeschreibung des Ereignises festgehalten ist soll versucht werden die Absturzursache zu kategorisieren:
# MAGIC 
# MAGIC - Andere
# MAGIC - Mechanik
# MAGIC - Pilotenfehler
# MAGIC - Wetter
# MAGIC - Sabotage
# MAGIC 
# MAGIC Anschliessend sollen die Abstürze nach folgende Kritieren untersucht werden:
# MAGIC 
# MAGIC - Fluggesellschaft
# MAGIC - Absturzursache
# MAGIC - Flugzeugtype
# MAGIC 
# MAGIC Dabei soll auch die Entwicklung über den Zeitraum des Datensatzes dargestellt werden.
# MAGIC 
# MAGIC Bei der Kategorisierung der Absturzursachen soll in einem ersten Anlauf überprüft werden ob mit einem einfachen Ansatz eine hohe Präzision der Kategorisierung erreicht werden kann (> 90%). Falls dies nicht funktioniert, soll evaluiert werden welche weiteren Ansätze zu einer höheren Präzision führen.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Python 2
# MAGIC * test_helper, siehe [here](https://databricks-staging-cloudfront.staging.cloud.databricks.com/public/c65da9a2fa40e45a2028cddebe45b54c/8637560089690848/4187311313936645/6977722904629137/05f3c2ecc3.html).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Kaggleprojekt:** https://www.kaggle.com/nguyenhoc/plane-crash
# MAGIC 
# MAGIC **File:** planecrashinfo_20181121001952.csv
# MAGIC 
# MAGIC **Stand:** 21.11.2018
# MAGIC 
# MAGIC **Beschreibung Dataset:**
# MAGIC 
# MAGIC <table style="width: 618.9px;" border="0" cellspacing="0" cellpadding="2" align="center">
# MAGIC <tbody>
# MAGIC <tr>
# MAGIC <td style="width: 78px;" align="right"><strong><span class="style10">Bezeichnung</span></strong></td>
# MAGIC <td style="width: 525.9px;"><strong><span class="style7">Beschreibung</span></strong></td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td style="width: 78px;" align="right"><span class="style10">Date:</span></td>
# MAGIC <td style="width: 525.9px;"><span class="style7">&nbsp;Date of accident, &nbsp;in the format - January 01, 2001</span></td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td style="width: 78px;" align="right"><span class="style10"> Time:</span></td>
# MAGIC <td style="width: 525.9px;"><span class="style7">&nbsp;Local time, in 24 hr. format unless otherwise specified</span></td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td style="width: 78px;" align="right"><span class="style10"> Airline/Op:</span></td>
# MAGIC <td style="width: 525.9px;"><span class="style7">&nbsp;Airline or operator of the aircraft</span></td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td style="width: 78px;" align="right"><span class="style10"> Flight #:</span></td>
# MAGIC <td style="width: 525.9px;"><span class="style7">&nbsp;Flight number assigned by the aircraft operator</span></td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td style="width: 78px;" align="right"><span class="style10"> Route:</span></td>
# MAGIC <td style="width: 525.9px;"><span class="style7">&nbsp;Complete or partial route flown prior to the accident</span></td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td style="width: 78px;" align="right"><span class="style10"> AC Type:</span></td>
# MAGIC <td style="width: 525.9px;"><span class="style7">&nbsp;Aircraft type</span></td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td style="width: 78px;" align="right"><span class="style10"> Reg:</span></td>
# MAGIC <td style="width: 525.9px;"><span class="style7">&nbsp;ICAO registration of the aircraft</span></td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td style="width: 78px;" align="right"><span class="style10"> cn / ln:</span></td>
# MAGIC <td style="width: 525.9px;"><span class="style7">&nbsp;Construction or serial number / Line or fuselage number</span></td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td style="width: 78px;" align="right"><span class="style10"> Aboard:</span></td>
# MAGIC <td style="width: 525.9px;"><span class="style7">&nbsp;Total aboard (passengers / crew)</span></td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td style="width: 78px;" align="right"><span class="style10"> Fatalities:</span></td>
# MAGIC <td style="width: 525.9px;"><span class="style7">&nbsp;Total fatalities aboard (passengers / crew)</span></td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td style="width: 78px;" align="right"><span class="style10"> Ground:</span></td>
# MAGIC <td style="width: 525.9px;"><span class="style7">&nbsp;Total killed on the ground</span></td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td style="width: 78px;" align="right"><span class="style10"> Summary:</span></td>
# MAGIC <td style="width: 525.9px;"><span class="style7">&nbsp;Brief description of the accident and cause if known</span></td>
# MAGIC </tr>
# MAGIC </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Für die Tests wurde die test_helper Klasse integriert.
# MAGIC 
# MAGIC Für die einzelnen Funktionen wurden Testfälle erstellt.
# MAGIC 
# MAGIC Um die Resultate sauber zu verifizieren, wurden die Daten auch noch in eine MySQL Datenbank geladen.
# MAGIC Die Abfragen wurden zur Überprüfung jeweils auf der MySQL DB laufen gelassen.

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

# File location and type
file_location = "/FileStore/tables/planecrashinfo_20181121001952.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
# .option("dateFormat", "MMM d, yy") seems not to work
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)


# Hence date conversion has to be done manually
dfcrashes = df.withColumn("date", to_date(df.date, 'MMM d, yy'))
display(dfcrashes)

# COMMAND ----------

temp_table = "crashestable"

dfcrashes.createOrReplaceTempView(temp_table)

# COMMAND ----------

def checkanzrecord():
  return dfcrashes.count()

print (checkanzrecord())

# COMMAND ----------

# TEST Ob die eingelesenen Records der erwarteten Anzahl Records entspricht
from test_helper import Test
Test.assertEquals(checkanzrecord(), 5784, 'incorrect Total Records')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from crashestable 
# MAGIC where date is not null
# MAGIC order by date asc, time asc
# MAGIC limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from crashestable 
# MAGIC where date is not null
# MAGIC order by date desc, time desc
# MAGIC limit 1

# COMMAND ----------

# MAGIC %md # Analyse der Daten

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) AS CrashesInSwitzerland 
# MAGIC from crashestable 
# MAGIC where lower(location) like '%switzerland%'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) as SwissRelatedCrashes
# MAGIC from crashestable 
# MAGIC where lower(operator) like 'swiss%' or
# MAGIC       lower(operator) like 'ju air'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table crashesred

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create table crashesred
# MAGIC select date,
# MAGIC        time,
# MAGIC        location,
# MAGIC        case operator
# MAGIC          when 'Lufthansa' then 'Deutsche Lufthansa'
# MAGIC          when 'Deutche Lufthansa' then 'Deutsche Lufthansa'
# MAGIC          when 'Lufthansa Cityline' then 'Deutsche Lufthansa'
# MAGIC          when 'Lufthansa Cargo Airlines' then 'Deutsche Lufthansa'
# MAGIC          when 'Eurowings' then 'Deutsche Lufthansa'
# MAGIC          when 'Germanwings' then 'Deutsche Lufthansa'
# MAGIC          else operator
# MAGIC        end as operator,
# MAGIC        flight_no,
# MAGIC        route,
# MAGIC        ac_type,
# MAGIC        registration,
# MAGIC        cn_ln,
# MAGIC        aboard,
# MAGIC        fatalities,
# MAGIC        ground,
# MAGIC        summary
# MAGIC from crashestable
# MAGIC where operator not like '%Military%' and 
# MAGIC       operator not like '%USMC%' and
# MAGIC       operator not like '%Marines%' and
# MAGIC       operator not like '%Navy%' and
# MAGIC       operator not like '%Private%'

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select operator,
# MAGIC        count(*) as anz
# MAGIC from crashesred
# MAGIC group by operator 
# MAGIC order by anz desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select floor(year(date)/10)*10 as decade,
# MAGIC        operator,
# MAGIC        count(*) as anz
# MAGIC from crashesred
# MAGIC where operator in ('Deutsche Lufthansa','Aeroflot','Air France','United Airlines','China National Aviation Corporation')
# MAGIC group by operator, floor(year(date)/10)*10
# MAGIC order by anz desc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table crashes_causes

# COMMAND ----------

# MAGIC %sql
# MAGIC create table crashes_causes
# MAGIC select *, 'Weather' as reason
# MAGIC from crashesred
# MAGIC where lower(summary) like '%severe turbulence%'  or
# MAGIC       lower(summary) like '%windshear%' or
# MAGIC       lower(summary) like '%mountain wave%' or
# MAGIC       lower(summary) like '%poor visibility%' or
# MAGIC       lower(summary) like '%heavy rain%' or 
# MAGIC       lower(summary) like '%severe winds%' or
# MAGIC       lower(summary) like '%icing%' or
# MAGIC       lower(summary) like '%thunderstorms%' or
# MAGIC       lower(summary) like '%lightning strike%'
# MAGIC union
# MAGIC select *, 'Sabotage' as reason
# MAGIC from crashesred
# MAGIC where lower(summary) like '%bomb%' or
# MAGIC       lower(summary) like '%hijack%' or
# MAGIC       lower(summary) like '%shot down%'
# MAGIC union
# MAGIC select *, 'Mechanical' as reason
# MAGIC from crashesred
# MAGIC where lower(summary) like '%engine failure%' or
# MAGIC       lower(summary) like '%equipment failure%' or
# MAGIC       lower(summary) like '%design flaw%' or
# MAGIC       lower(summary) like '%maintenance error%'      
# MAGIC union
# MAGIC select *, 'Pilot Error' as reason
# MAGIC from crashesred
# MAGIC where lower(summary) like '%improper procedure%' or
# MAGIC       lower(summary) like '%flying vfr into ifr conditions%' or
# MAGIC       lower(summary) like '%controlled flight into terrain%' or
# MAGIC       lower(summary) like '%descending below minima%' or
# MAGIC       lower(summary) like '%spatial disorientation%' or
# MAGIC       lower(summary) like '%premature descent%' or
# MAGIC       lower(summary) like '%excessive landing speed%' or
# MAGIC       lower(summary) like '%missed runway%' or
# MAGIC       lower(summary) like '%fuel starvation%' or
# MAGIC       lower(summary) like '%navigation error%' or
# MAGIC       lower(summary) like '%wrong runway takeoff/landing%' or
# MAGIC       lower(summary) like '%midair collision caused by both pilots%'
# MAGIC union
# MAGIC select *, 'Other' as reason
# MAGIC from crashesred
# MAGIC where lower(summary) like '%atc error%' or
# MAGIC       lower(summary) like '%ground crew error%' or
# MAGIC       lower(summary) like '%overloaded%' or
# MAGIC       lower(summary) like '%improperly loaded cargo%' or
# MAGIC       lower(summary) like '%bird strike%' or
# MAGIC       lower(summary) like '%fuel contamination%' or
# MAGIC       lower(summary) like '%pilot incapacitation%' or
# MAGIC       lower(summary) like '%obstruction on runway%' or
# MAGIC       lower(summary) like '%midair collision caused by other plane%' or
# MAGIC       lower(summary) like '%fire/smoke in flight%' or
# MAGIC       lower(summary) like '%fire in flight%' or
# MAGIC       lower(summary) like '%smoke in flight%'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select floor(year(date)/10)*10 as decade,
# MAGIC        reason,
# MAGIC        count(reason)
# MAGIC from crashes_causes
# MAGIC group by floor(year(date)/10)*10, reason
# MAGIC order by floor(year(date)/10)*10

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Um heraus zu finden wie präzise dieser einfache Kategorisierungsansatz ist, wurden jeweils 50 Records herausgesucht, deren Kategorisierung wurden manuell überprüft, dabei hat sich folgendes herausgestellt:
# MAGIC 
# MAGIC - Keine Records ohne Kategorisierung
# MAGIC - Datensample A ergibt eine Präzision von 96% bei der Kategorisierung
# MAGIC - Datensample B ergibt eine Präzision von 98% bei der Kategorisierung
# MAGIC 
# MAGIC Somit zeigt sich das schon ein einfacher Ansatz zur Kategorisierung der Absturzursachen zu einer sehr hohen Präzision führt. Daher ist ein ML basierender Ansatz in diesem Fall gar nicht nötig.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) 
# MAGIC from crashes_causes 
# MAGIC where reason is null
# MAGIC 
# MAGIC /*Yippie keine Nuller*/

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select summary, reason, operator
# MAGIC from crashes_causes
# MAGIC order by year(date) desc
# MAGIC limit 50

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from crashes_causes
# MAGIC order by year(date) asc
# MAGIC limit 50

# COMMAND ----------

# MAGIC %md #Zusätzlich den Flugzeugtype den grössten Flugzeugherstellern zuweisen

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table crashes_manufacturer

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC create table crashes_manufacturer
# MAGIC select *, 'Airbus' as Manufacturer
# MAGIC from crashes_causes
# MAGIC where lower(ac_type) like '%airbus%'
# MAGIC union
# MAGIC select *, 'ATR' as Manufacturer
# MAGIC from crashes_causes
# MAGIC where lower(ac_type) like '%atr%'
# MAGIC union
# MAGIC select *, 'Antonov' as Manufacturer
# MAGIC from crashes_causes
# MAGIC where lower(ac_type) like '%antonov%'
# MAGIC union
# MAGIC select *, 'Beechcraft' as Manufacturer
# MAGIC from crashes_causes
# MAGIC where lower(ac_type) like '%beech%'
# MAGIC union
# MAGIC select *, 'Boeing' as Manufacturer
# MAGIC from crashes_causes
# MAGIC where lower(ac_type) like '%boeing%'
# MAGIC union
# MAGIC select *, 'British Aerospace' as Manufacturer
# MAGIC from crashes_causes
# MAGIC where lower(ac_type) like '%british aerospace%' or
# MAGIC       lower(ac_type) like '%bae%'
# MAGIC union
# MAGIC select *, 'Cessna' as Manufacturer
# MAGIC from crashes_causes
# MAGIC where lower(ac_type) like '%cessna%'
# MAGIC union
# MAGIC select *, 'Embraer' as Manufacturer
# MAGIC from crashes_causes
# MAGIC where lower(ac_type) like '%embraer%'
# MAGIC union
# MAGIC select *, 'Fokker' as Manufacturer
# MAGIC from crashes_causes
# MAGIC where lower(ac_type) like '%fokker%'
# MAGIC union
# MAGIC select *, 'Ilyushin' as Manufacturer
# MAGIC from crashes_causes
# MAGIC where lower(ac_type) like '%ilyushin%'
# MAGIC union
# MAGIC select *, 'Junkers' as Manufacturer
# MAGIC from crashes_causes
# MAGIC where lower(ac_type) like '%junkers%'
# MAGIC union
# MAGIC select *, 'Lookheed Martin' as Manufacturer
# MAGIC from crashes_causes
# MAGIC where lower(ac_type) like '%lookheed%' or
# MAGIC       lower(ac_type) like '%martin%'
# MAGIC union
# MAGIC select *, 'McDonnell Douglas' as Manufacturer
# MAGIC from crashes_causes
# MAGIC where lower(ac_type) like '%douglas%' or
# MAGIC       lower(ac_type) like '%mcdonnell%' or
# MAGIC       lower(ac_type) like '%md%'
# MAGIC union
# MAGIC select *, 'Piper' as Manufacturer
# MAGIC from crashes_causes
# MAGIC where lower(ac_type) like '%piper%' 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select manufacturer,
# MAGIC        floor(year(date)/10)*10 as Decade,
# MAGIC        count(*) as Crashes
# MAGIC from crashes_manufacturer
# MAGIC group by manufacturer, floor(year(date)/10)*10
# MAGIC order by floor(year(date)/10)*10 asc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select ac_type, count(ac_type)
# MAGIC from crashes_causes
# MAGIC group by ac_type
# MAGIC order by count(ac_type) desc
