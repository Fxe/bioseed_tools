
all:
	echo "wut"

build:
	docker build -t bioseed_kofam_scan .

start:
	docker run -it --name bioseed_kofam_scan --hostname bioseed_kofam_scan -v /var/data/network/kofam:/host --network bioseed_network bioseed_kofam_scan:latest

clean:
	docker stop bioseed_kofam_scan
	docker rm bioseed_kofam_scan
