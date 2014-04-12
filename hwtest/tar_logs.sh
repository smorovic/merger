## Source this file to tar the logs in /lustre/$NAME into a tarball /lustre/${NAME}.tgz
## The variable $NAME must be properly defined
pushd /lustre
tar czvf ${NAME}.tgz $NAME
du -skh ${NAME}*
popd
