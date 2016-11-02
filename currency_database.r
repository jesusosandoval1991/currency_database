filename1 <- "C:\\Users\\sandovaj.AUTH\\Desktop\\currency_database\\equity_currency_08312016.csv"
filename2 <- "C:\\Users\\sandovaj.AUTH\\Desktop\\currency_database\\security_currency_08312016.csv"
filename3 <- "C:\\Users\\sandovaj.AUTH\\Desktop\\currency_database\\detailed_report_08312016.csv"
filename4 <- "C:\\Users\\sandovaj.AUTH\\Desktop\\currency_database\\counterparty_currency_08312016.csv"
filename5 <- "C:\\Users\\sandovaj.AUTH\\Desktop\\currency_database\\bb_component_08312016.csv"

equity_currency <- read.csv(filename1,header=TRUE,sep=",") 
security_currency <- read.csv(filename2,header=TRUE,sep=",") 
detailed_report <- read.csv(filename3,header=TRUE,sep=",")
counterparty_currency <- read.csv(filename4, header=TRUE,sep=",")
bb_component <- read.csv(filename5,header=TRUE,sep=",")

#Edit the formatting to use SQL to drive analysis #Names are too long, all headers need to be
#renamed 

colnames(equity_currency) <-c("account_number","account_name","date","currency_name","tot_acc_mkt_val","currency_code")

colnames(security_currency) <- c("date","currency_name","accrued_mkt_val","currency_code","security","source_account_number",
				 "source_account_name","report_account_number","report_account_name") 

colnames(detailed_report) <- c("report_account_number", "report_account_name", "date", "currency_name","tot_mkt_val_loc",
			       "tot_rec_mkt_val_loc", "tot_mkt_val_base", "tot_rec_mkt_val_base","tot_accrued_mkt_val",
			       "currency_code","avg_local_base_ex_rate")

colnames(counterparty_currency) <- c("report_account_number","report_account_name", "source_account_number",
			             "source_account_name","date","payable_currency_code","rec_currency_code",
				     "payable_units","receiveable_units")

#Will be using SQL to drive the process
library(sqldf)
detailed_report$tot_accrued_mkt_val <- gsub('[,]','',detailed_report$tot_accrued_mkt_val)
a <- sqldf("select date, currency_name, currency_code, tot_accrued_mkt_val from detailed_report 
				group by currency_code order by currency_name")

#Found a bug, the commas in the accrued mkt value comment throw off calculations, need to be reformatted
security_currency$accrued_mkt_val <- gsub('[,]','',security_currency$accrued_mkt_val)
b <- sqldf("select date, currency_code, currency_name, sum(accrued_mkt_val) tot_mkt_val from security_currency
	    group by currency_code order by currency_name")

#Merges data accrued so far
c <- sqldf("select a.date, a.currency_code, a.currency_name,a.tot_accrued_mkt_val,b.tot_mkt_val from a 
			inner join b on a.currency_code = b.currency_code")

#Counterparty exposure

counterparty_currency$payable_units <- gsub('[,]','',counterparty_currency$payable_units)
d <- sqldf("select payable_currency_code, sum(payable_units) tot_pay_units from counterparty_currency where 
		source_account_name!='NISA FX OVERLAY' group by payable_currency_code")
colnames(d)[1] <- "currency_code"

counterparty_currency$receiveable_units <- gsub('[,]','',counterparty_currency$receiveable_units)
e <- sqldf("select rec_currency_code, sum(receiveable_units) tot_rec_units from counterparty_currency where 
		source_account_name!='NISA FX OVERLAY' group by rec_currency_code")
colnames(e)[1] <- "currency_code"

f <- merge(x=d,y=e,by="currency_code",all=TRUE)

#Get rid of the NA values after the merge
f[is.na(f)] <- 0

#Figure out how many fx forwards we have
f['fx_forwards_local'] <- f$tot_pay_unit + f$tot_rec_units
f <- merge(x=c,y=f,by="currency_code",all=TRUE)
f['match'] <- f$tot_accrued_mkt_val==f$tot_mkt_val
f[is.na(f)] <- 0

# Let's get rid of what we don't need anymore
f$tot_pay_units <- NULL
f$tot_rec_units <- NULL

#For presentation purposes, let's rearrange the columns

f <- sqldf("select date, currency_code, currency_name, tot_accrued_mkt_val, tot_mkt_val, match, fx_forwards_local from 
			f order by currency_name ")

#Let's get a list of all the currencies we are active in and let's get their end-of-month fx rate
#bb_currencies <- sqldf("select date, currency_code from f") 
#write.csv(bb_currencies,"C:\\Users\\sandovaj.AUTH\\Desktop\\currency_database\\bb_component.csv")

#Let's incorporate bloomberg values
f <- sqldf("select f.date,f.currency_code,f.currency_name,f.tot_accrued_mkt_val,f.tot_mkt_val,f.match, f.fx_forwards_local,
			bb_component.fx_rate,bb_component.flip from f inner join bb_component on f.currency_code = bb_component.currency_code
			order by currency_name")

#Formatting first
f$flip <- as.character(f$flip)
f$fx_rate <- as.numeric(as.character(f$fx_rate))

#Find fx fowards in USD, this going to be an itireative function b/c we need to involve different parts along the way
get_fx_fwd_usd <- function(f) {
	f['flip_num'] <- 0
	count <- 1
	for(i in f$flip) {
		if(i =="Y") { f$flip_num[count] <- 1 } 
		else { f$flip_num[count] <- 0 }
		count <- count + 1
	}
	f['fx_fwd_usd'] <- 0
	count <- 1
	for(i in f$flip_num) {
		if(i ==1) { f$fx_fwd_usd[count] <- f$fx_forwards_local[count] * f$fx_rate[count]} 
		else if(i==0) {f$fx_fwd_usd[count] <- f$fx_forwards_local[count] / f$fx_rate[count]} 
		count <- count + 1
	}
	return(f)
}

f <- get_fx_fwd_usd(f)

#Let's get the total USD value exposure to different currencies
f['tot_exp'] <- 0
f$tot_exp <- as.numeric(f$tot_mkt_val) + f$fx_fwd_usd

#toss out what we no longer need
f$flip_num <- NULL
#Revert the column names to their original names back to their original values  
write.csv(f,"C:\\Users\\sandovaj.AUTH\\Desktop\\currency_database\\tot_currency_exposure_09302016.csv")

#What we send out to NISA
nisa_report <- subset(f,currency_code=="EUR"| currency_code=="GBP"|currency_code=="CHF"|currency_code=="SEK"|
			currency_code=="AUD"|currency_code=="JPY")

nisa_report <- sqldf("select date, currency_name, currency_code, tot_exp from nisa_report")

write.csv(nisa_report,"C:\\Users\\sandovaj.AUTH\\Desktop\\currency_database\\nisa_report_09302016.csv") 
