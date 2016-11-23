#!/usr/bin/env Rscript
library(reshape2)
library(ggplot2)
library(plyr)
library(RColorBrewer)
library(gtools)

args <- commandArgs(trailingOnly = TRUE)

print(args)

if (length(args)!=6) {
  stop("The script requires exactly 6 parameters!\nUsage: <script.r> output_file missing_impute_value n_histogram_bins n_color_bins width_inches height_inches\nExample: script.r outputfile.pdf -110 20 7 8 6")
}

output_file = "plot.pdf"
missing_impute_value = -110
n_histogram_bins = 7
n_color_bins = 7
width_inches = 6
height_inches = 6

output_file = args[1]
missing_impute_value = as.numeric(args[2])
n_histogram_bins = as.numeric(args[3])
n_color_bins = as.numeric(args[4])
width_inches = as.numeric(args[5])
height_inches = as.numeric(args[6])

f <- file("stdin")
open(f)
d = read.csv(f)

d = d[d$location!="MIX",]
d$location = factor(d$location)

dm = melt(d[,-1])

dma = dm
dma[is.na(dma)] = missing_impute_value
dmx = ddply(dma, c("location","variable"), function(df) mean(df$value))
# dmx$value_discrete = quantcut(dmx$V1,n_color_bins)
dmx$value_discrete = cut(dmx$V1,breaks=c(-120,-110,-100,-90,-80,-70,-60,-50))

print(levels(dmx$value_discrete))

p = ggplot(dmx)
p = p + facet_grid(location~variable,scales="free_y")
p = p + geom_rect(aes(fill=value_discrete),xmin = -Inf,xmax = Inf, ymin = -Inf,ymax = Inf,alpha = 0.8)
p = p + scale_fill_brewer(palette="RdBu",direction=-1,drop=F)
p = p + geom_histogram(data=dma,aes(value),bins=n_histogram_bins)

#g <- file("stdout")
#g <- stdout()
ggsave(filename=output_file,plot=p,width=width_inches,height=height_inches) #,device="pdf")

