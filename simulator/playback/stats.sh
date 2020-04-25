#!/usr/bin/env bash

LOG=$1


echo SET stats
SET=$(cat ${LOG} | grep NormalSet | wc -l)
echo ${SET}

echo GET stats
GET=$(cat ${LOG} | grep Get | wc -l)
GET=$((${GET}/2))
echo ${GET}

echo Reset \(GET miss\) stats
RESET=$(cat ${LOG} | grep ResetSet | wc -l)
echo ${RESET}

echo Hit Rate
TOTAL=$((${GET}+${SET}+${RESET}))
echo ${GET}, ${TOTAL}
