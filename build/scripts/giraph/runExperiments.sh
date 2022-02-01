#!/bin/bash

SCRIPT_HOME="./scripts/giraph"

declare -A outputGrid

algos=("EAT" "LD" "SSSP" "TR" "TMST" "FAST")
modes=("ICM_LUDS" "WICM" "WICM_LUDS")

for ((i=0;i<${#modes[@]};i++)) do
    for ((j=0;j<${#algos[@]};j++)) do
        m=${modes[$i]}
        a=${algos[$j]}
        outputGrid[$m","$a]=0
    done
done

sources=("22499" "19862")
windows=("0;20;30;40" "0;21;40")
bufferSize="100"
minMsg="20"

for a in "${algos[@]}"
do
  echo $a
  echo "========================="
  for s in "${sources[@]}"
  do
    echo $s
    echo "--------------------------"
    
    bash $SCRIPT_HOME/icm/run"$a".sh "$s" false sampleGraph.txt ICM # run native ICM
    if [ ! -f ICM/sorted.txt ]; then # check if output dumped
      >&2 echo "Problem in ICM $a $s"
      break
    fi

    bash $SCRIPT_HOME/icm_luds/run"$a".sh "$s" $bufferSize $minMsg false sampleGraph.txt ICM_LUDS # run native ICM+LU+DS
    if [ ! -f ICM_LUDS/sorted.txt ]; then # check if output dumped
      >&2 echo "Problem in ICM_LUDS $a $s"
    else
      bash $SCRIPT_HOME/compare.sh ICM ICM_LUDS # compare output
      correct=$(echo "$?")
      if [[ "$correct" == "0" ]]; then
        outputGrid["ICM_LUDS,"$a]=$((outputGrid["ICM_LUDS,"$a]+1))
      else
        >&2 echo "$s $a ICM ICM_LUDS Not equivalent"
      fi
      rm -r ICM_LUDS
    fi

    for w in "${windows[@]}"
    do
      
      bash $SCRIPT_HOME/wicm/run"$a".sh "$s" 0 40 "$w" false sampleGraph.txt WICM # run WICM
      if [ ! -f WICM/sorted.txt ]; then # check if output dumped
        >&2 echo "Problem in WICM $a $s"
      else
        bash $SCRIPT_HOME/compare.sh ICM WICM # compare output
        correct=$(echo "$?")
        if [[ "$correct" == "0" ]]; then
          outputGrid["WICM,"$a]=$((outputGrid["WICM,"$a]+1))
        else
          >&2 echo "$s $a ICM WICM Not equivalent"
        fi
        rm -r WICM
      fi

      bash $SCRIPT_HOME/wicm_luds/run"$a".sh "$s" 0 40 "$w" $bufferSize $minMsg false sampleGraph.txt WICM_LUDS # run WICM+LU+DS
      if [ ! -f WICM_LUDS/sorted.txt ]; then # check if output dumped
        >&2 echo "Problem in WICM_LUDS $a $s"
      else
        bash $SCRIPT_HOME/compare.sh ICM WICM_LUDS # compare output
        correct=$(echo "$?")
        if [[ "$correct" == "0" ]]; then
          outputGrid["WICM_LUDS,"$a]=$((outputGrid["WICM_LUDS,"$a]+1))
        else
          >&2 echo "$s $a ICM WICM_LUDS Not equivalent"
        fi
        rm -r WICM_LUDS
      fi

    done

    rm -r ICM
  done
done

echo " ""${algos[*]}" >> experiment.log
for ((i=0;i<${#modes[@]};i++)) do
  m=${modes[$i]}
  echo -n $m >> experiment.log
  for ((j=0;j<${#algos[@]};j++)) do
      a=${algos[$j]}
      echo -n " "${outputGrid[$m,$a]} >> experiment.log
  done
  echo "" >> experiment.log
done
