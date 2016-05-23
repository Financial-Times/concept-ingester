FROM alpine:3.3

ADD *.go /concept-ingester/

RUN apk add --update bash \
  && apk --update add git go \
  && cd concept-ingester\
  && git fetch origin 'refs/tags/*:refs/tags/*' \
  && BUILDINFO_PACKAGE="github.com/Financial-Times/service-status-go/buildinfo." \
  && VERSION="version=$(git describe --tag --always 2> /dev/null)" \
  && DATETIME="dateTime=$(date -u +%Y%m%d%H%M%S)" \
  && REPOSITORY="repository=$(git config --get remote.origin.url)" \
  && REVISION="revision=$(git rev-parse HEAD)" \
  && BUILDER="builder=$(go version)" \
  && LDFLAGS="-X '"${BUILDINFO_PACKAGE}$VERSION"' -X '"${BUILDINFO_PACKAGE}$DATETIME"' -X '"${BUILDINFO_PACKAGE}$REPOSITORY"' -X '"${BUILDINFO_PACKAGE}$REVISION"' -X '"${BUILDINFO_PACKAGE}$BUILDER"'" \
  && cd .. \
  && export GOPATH=/gopath \
  && REPO_PATH="github.com/Financial-Times/concept-ingester" \
  && mkdir -p $GOPATH/src/${REPO_PATH} \
  && cp -r concept-ingester/* $GOPATH/src/${REPO_PATH} \
  && cd $GOPATH/src/${REPO_PATH} \
  && go get ./... \
  && cd $GOPATH/src/${REPO_PATH} \
  && echo ${LDFLAGS} \
  && go build -ldflags="${LDFLAGS}" \
  && mv concept-ingester /app \
  && apk del go git \
  && rm -rf $GOPATH /var/cache/apk/*
CMD exec /app --services-list=$SERVICES --port=$APP_PORT --env=local --consumer_proxy_address=$PROXY_ADDR --consumer_group_id=$GROUP_ID --consumer_offset= --consumer_autocommit_enable=false --topic=$TOPIC
