hw6.html: hw6.Rmd plots/combo_plot.png plots/yellow_pickup.png \
plots/yellow_dropoff.png plots/green_pickup.png plots/green_dropoff.png \
plots/green_rush_pickup.png plots/yellow_rush_pickup.png
	Rscript -e "rmarkdown::render('hw6.Rmd')"

plots/green_rush_pickup.png plots/yellow_rush_pickup.png plots/yellow_pickup.png plots/yellow_dropoff.png plots/green_pickup.png plots/green_dropoff.png plots/combo_plot.png: generate_heatmaps.R \
data/yellow.csv data/green.csv
	mkdir -p $(@D)
	Rscript $<

data/yellow.csv data/green.csv: pull_data.R
	mkdir -p $(@D)
	Rscript $<

clean_html:
	rm hw6.html

clean_plots:
	rm -rf plots/
	
clean_data:
	rm -rf data/

.PHONY: clean_plots; clean_html; clean_data