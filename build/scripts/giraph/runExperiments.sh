#!/bin/bash

SCRIPT_HOME="./"

algos=("EAT" "LD" "SSSP" "TR" "TMST" "FAST")
sources=("22499" "19862")
windows=("0;20;30;40" "0;21;40")

bufferSize="100"
minMsg="20"

for a in "${algos[@]}"
do
  for s in "${sources[@]}"
  do
    bash $SCRIPT_HOME/icm/run"$a".sh "$s" false sampleGraph.txt ICM # run native ICM

    bash $SCRIPT_HOME/icm_luds/run"$a".sh "$s" $bufferSize $minMsg false sampleGraph.txt ICM_LUDS # run native ICM+LU+DS
    bash $SCRIPT_HOME/compare.sh ICM_debug ICM_LUDS_debug # compare output
    correct=$(echo "$?")
    if [[ "$correct" == "0" ]]; then
      rm -r ICM_LUDS_debug
    else
      echo "$s $a ICM ICM_LUDS Not equivalent"
      exit 1
    fi

    for w in "${windows[@]}"
    do
      bash $SCRIPT_HOME/wicm/run"$a".sh "$s" 0 40 "$w" false sampleGraph.txt WICM # run WICM
      bash $SCRIPT_HOME/compare.sh ICM_debug WICM_windowed # compare output
      correct=$(echo "$?")
      if [[ "$correct" == "0" ]]; then
        rm -r WICM_windowed
      else
        echo "$s $a ICM WICM Not equivalent"
        exit 1
      fi

      bash $SCRIPT_HOME/wicm_luds/run"$a".sh "$s" 0 40 "$w" $bufferSize $minMsg false sampleGraph.txt WICM_LUDS # run WICM+LU+DS
      bash $SCRIPT_HOME/compare.sh ICM_debug WICM_LUDS_windowed # compare output
      correct=$(echo "$?")
      if [[ "$correct" == "0" ]]; then
        rm -r WICM_LUDS_windowed
      else
        echo "$s $a ICM WICM_LUDS Not equivalent"
        exit 1
      fi
    done

    rm -r ICM_debug
  done
done
