import org.apache.spark.sql.types._;
import org.apache.spark.sql.expressions.Aggregator;

var ds = spark.read.option("header","true").csv("Input_StartOfDay_Positions.txt");
ds.createOrReplaceTempView("Input_StartOfDay_Positions");


var json = sc.wholeTextFiles("Input_Transactions.txt").map(tuple => tuple._2.replace("\n", "").trim);
var sqlContext = new org.apache.spark.sql.SQLContext(sc);
var df = sqlContext.read.json(json);
df.createOrReplaceTempView("Input_Transactions");

val df_tot = spark.sql("SELECT Instrument, SUM(TransactionQuantity) as TransactionQuantity, TransactionType as TransactionType FROM Input_Transactions GROUP BY Instrument, TransactionType");
df_tot.createOrReplaceTempView("Input_Transactions_Tot");

val eod_pos_detail = spark.sql("SELECT IP.*, IT.TransactionType, IT.TransactionQuantity FROM Input_StartOfDay_Positions IP LEFT JOIN Input_Transactions_Tot IT on IP.Instrument = IT.Instrument  " );
eod_pos_detail.createOrReplaceTempView("eod_pos_detail");

val eod_pos_intra = spark.sql("SELECT Instrument, Account, AccountType, case when (TransactionType = 'B' and AccountType = 'E') then  ( Quantity+TransactionQuantity ) when (TransactionType = 'B' and AccountType = 'I') then  ( Quantity-TransactionQuantity ) when (TransactionType = 'S' and AccountType = 'E') then  ( Quantity-TransactionQuantity ) when (TransactionType = 'S' and AccountType = 'I') then  ( Quantity+TransactionQuantity ) else Quantity end  as TotQty, case when (TransactionType = 'B' and AccountType = 'E') then  (0+TransactionQuantity ) when (TransactionType = 'B' and AccountType = 'I') then  ( 0-TransactionQuantity ) when (TransactionType = 'S' and AccountType = 'E') then  ( 0-TransactionQuantity ) when (TransactionType = 'S' and AccountType = 'I') then  ( 0+TransactionQuantity ) else 0 end  as Delta FROM eod_pos_detail  ");

eod_pos_intra.createOrReplaceTempView("eod_pos_intra");

val eod_pos = spark.sql("SELECT Instrument, Account, AccountType , Int(SUM(TotQty)) as Quantity, SUM(Delta) as Delta FROM eod_pos_intra GROUP BY Instrument, Account, AccountType " );

eod_pos.write.format("csv").save("end_of_day_positions.txt");
