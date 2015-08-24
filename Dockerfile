FROM scratch

MAINTAINER Guilherme Santos <guilherme.santos@neoway.com.br>

ADD ./es-reindex /opt/es-reindex/bin/

CMD ["/opt/es-reindex/bin/es-reindex"]
