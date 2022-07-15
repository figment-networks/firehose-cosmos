#!/usr/bin/env bash

set -o errexit
set -o pipefail

CLEANUP=${CLEANUP:-"0"}

GREEN='\e[0;32m'
RESET='\e[0m'
RED='\e[0;31m'

function log {
    echo -e "${GREEN} /-------------------------------------------------------------------------------------- ${RESET}"
    echo -e "${GREEN} - $@ ${RESET}\n"
}

function die {
    echo -e "${RED} /-------------------------------------------------------------------------------------- ${RESET}"
    echo -e "${RED} - $@ ${RESET}\n"
    exit 1
}

PASSWORD=${PASSWORD:-1234567890}
CHAIN_ID=${CHAIN_ID:-juno-1}
MONIKER_NAME="$(hostname)"
JUNO_HOME="juno_home"
JUNO_GENESIS_HEIGHT="2578099"
CHAIN_REPO="https://raw.githubusercontent.com/CosmosContracts/mainnet/main/$CHAIN_ID" && \
export PEERS="$(curl -s "$CHAIN_REPO/persistent_peers.txt")"

if [[ $CLEANUP -eq "1" ]]; then
  log "Deleting all local data"
  rm -rf ./tmp/ > /dev/null
fi


log "Setting up working directory"
mkdir -p tmp
pushd tmp

log "Copy junod to tmp"
if [ $(which junod) ]; then
  cp $(which junod) ./
else
  die "junod not found in PATH. Please install junod before running this script."
fi

log "Initialize the chain"
if [ ! -d ${JUNO_HOME} ]; then
  junod init "$MONIKER_NAME" --chain-id $CHAIN_ID --home ${JUNO_HOME}
fi

log "Download the genesis file"
if [ ! -f ${JUNO_HOME}/config/.genesis.downloaded ]; then
  curl -s https://share.blockpane.com/juno/phoenix/genesis.json > ${JUNO_HOME}/config/genesis.json
  touch ${JUNO_HOME}/config/.genesis.downloaded
fi

log "Set persistent peers"
sed -i -e "s/^persistent_peers *=.*/persistent_peers = \"$PEERS\"/" ${JUNO_HOME}/config/config.toml

log "Set Halt Height"
sed -i -e "s/^halt-height = 0$/halt-height = 2616300/" ${JUNO_HOME}/config/app.toml

log "Set the minimum gas prices"
sed -i -e "s/^minimum-gas-prices *=.*/minimum-gas-prices = \"0.0025ujuno,0.001ibc\/C4CFF46FD6DE35CA4CF4CE031E643C8FDC9BA4B99AE598E9B0ED98FE3A2319F9\"/" ${JUNO_HOME}/config/app.toml

log "Create a local key pair"
if ! junod --home ${JUNO_HOME} --keyring-backend test keys show validator > /dev/null 2>&1 ; then
  (echo "$PASSWORD"; echo "$PASSWORD") | junod --home ${JUNO_HOME} --keyring-backend test keys add validator
fi

log "Adding extractor config to config.yml"
if ! grep --silent 'Extractor Configuration Options' ${JUNO_HOME}/config/config.toml; then
  cat << END >> ${JUNO_HOME}/config/config.toml

#######################################################
###       Extractor Configuration Options     ###
#######################################################
[extractor]
enabled = true
output_file = "stdout"

END
fi

if [ ! -f "firehose.yml" ]; then
  log "Creating firehose.yml config file"
  cat << END > firehose.yml
start:
  args:
    - ingestor
    - merger
    - firehose
  flags:
    common-first-streamable-block: $JUNO_GENESIS_HEIGHT
    common-blockstream-addr: localhost:9000
    ingestor-mode: node
    ingestor-node-path: ./junod
    ingestor-node-args: start --x-crisis-skip-assert-invariants --home=./juno_home
    ingestor-node-logs-filter: "module=(p2p|pex|consensus|x/bank)"
    firehose-real-time-tolerance: 99999h
    relayer-max-source-latency: 99999h
    verbose: 1

END
fi
