REPO_NAME='coinbase-spark'
PREFIX='balis'

all: push

container: image

image:
	docker build -t $(PREFIX)/$(REPO_NAME) . # Build new image and automatically tag it as latest

run:
	docker network prune && docker network create coin-net && docker run --rm -p 8888:8888 -v $(PWD):/home/jovyan/work --network="coin-net" --name coinbase-notebook $(PREFIX)/$(REPO_NAME):latest start-notebook.sh --NotebookApp.token=''

push: image
	docker push $(PREFIX)/$(REPO_NAME) # Push image tagged as latest to repository

clean:

