all: sr_main1 sr_main2 sr_main3

sr_main1:
	@echo " Compile sr_main1 ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/sr_main1.c ./src/sort_file.c -lbf -o ./build/sr_main1 -O2
	find -name unsorted_data.db -print0 | xargs -0 -I file mv file ./last_unsorted_data.db

sr_main2:
	@echo " Compile sr_main2 ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/sr_main2.c ./src/sort_file.c -lbf -o ./build/sr_main2 -O2
	find -name sorted_id.db -print0 | xargs -0 -I file mv file ./last_sorted_id.db
	find -name sorted_name.db -print0 | xargs -0 -I file mv file ./last_sorted_name.db
	find -name sorted_surname.db -print0 | xargs -0 -I file mv file ./last_sorted_surname.db

sr_main3:
	@echo " Compile sr_main3 ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/sr_main3.c ./src/sort_file.c -lbf -o ./build/sr_main3 -O2

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c -lbf -o ./build/runner -O2
