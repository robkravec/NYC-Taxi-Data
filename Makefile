NYC_Taxi_Data.html: NYC_Taxi_Data.Rmd plots/combo_plot.png plots/yellow_pickup.png \
plots/yellow_dropoff.png plots/green_pickup.png plots/green_dropoff.png 
	Rscript -e "rmarkdown::render('NYC_Taxi_Data.Rmd')"
	
plots/yellow_pickup.png plots/yellow_dropoff.png plots/green_pickup.png plots/green_dropoff.png plots/combo_plot.png: generate_heatmaps.R \
data/yellow.csv data/green.csv
	mkdir -p $(@D)
	Rscript $<

data/yellow.csv data/green.csv: pull_data.R
	mkdir -p $(@D)
	Rscript $<

clean_html:
	rm NYC_Taxi_Data.html

clean_plots:
	rm -rf plots/
	
clean_data:
	rm -rf data/

.PHONY: clean_plots; clean_html; clean_data