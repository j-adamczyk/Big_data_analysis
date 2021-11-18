REPO_NAME=ws-streamer
PREFIX=balis

all: push

container: image

image:
	docker build -t  $(PREFIX)/$(REPO_NAME) . # Build new image and automatically tag it as latest

run:
	docker run --rm --network="coin-net" --name mock -it $(PREFIX)/$(REPO_NAME):latest Mock.scala

push: image
	docker push $(PREFIX)/$(REPO_NAME) # Push image tagged as latest to repository

clean:
