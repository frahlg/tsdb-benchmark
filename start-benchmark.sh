#!/bin/bash
# TSDB Benchmark Suite Launcher
# Usage: ./start-benchmark.sh [command]
#
# Commands:
#   up        - Start all databases
#   down      - Stop all databases
#   test      - Run benchmarks
#   clean     - Remove all data volumes
#   logs      - Show logs
#   status    - Check container status

set -e

cd "$(dirname "$0")"

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_header() {
    echo -e "\n${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  TSDB Benchmark Suite - Sourceful Energy${NC}"
    echo -e "${BLUE}  Target: 10,000 DERs @ 1s interval${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}\n"
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}Error: Docker is not installed${NC}"
        exit 1
    fi
    if ! docker info &> /dev/null; then
        echo -e "${RED}Error: Docker daemon is not running${NC}"
        exit 1
    fi
}

wait_for_service() {
    local name=$1
    local url=$2
    local max_attempts=${3:-60}
    local attempt=1

    echo -n "  $name: "
    while [ $attempt -le $max_attempts ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            echo -e "${GREEN}✓${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
        ((attempt++))
    done
    echo -e " ${RED}✗ (timeout)${NC}"
    return 1
}

start_databases() {
    echo -e "${GREEN}Starting databases...${NC}"
    docker compose up -d

    echo -e "\n${GREEN}Waiting for databases to be ready...${NC}"

    # Use docker compose health checks
    echo -e "${YELLOW}Waiting for containers to become healthy...${NC}"

    local max_wait=120
    local elapsed=0

    while [ $elapsed -lt $max_wait ]; do
        local all_healthy=true

        for service in victoriametrics questdb clickhouse timescaledb; do
            local health=$(docker inspect --format='{{.State.Health.Status}}' $service 2>/dev/null || echo "unknown")
            if [ "$health" != "healthy" ]; then
                all_healthy=false
            fi
        done

        if $all_healthy; then
            break
        fi

        echo -n "."
        sleep 2
        ((elapsed+=2))
    done
    echo ""

    # Final status check
    echo -e "\n${GREEN}Service Status:${NC}"
    for service in victoriametrics questdb clickhouse timescaledb grafana; do
        local status=$(docker inspect --format='{{.State.Status}}' $service 2>/dev/null || echo "not found")
        local health=$(docker inspect --format='{{.State.Health.Status}}' $service 2>/dev/null || echo "n/a")

        if [ "$status" == "running" ]; then
            echo -e "  $service: ${GREEN}running${NC} (health: $health)"
        else
            echo -e "  $service: ${RED}$status${NC}"
        fi
    done

    echo -e "\n${GREEN}Access points:${NC}"
    echo -e "  VictoriaMetrics: http://localhost:8428/vmui"
    echo -e "  QuestDB:         http://localhost:9000"
    echo -e "  ClickHouse:      http://localhost:8123/play"
    echo -e "  TimescaleDB:     postgresql://postgres:postgres@localhost:5432/sourceful"
    echo -e "  Grafana:         http://localhost:3000 (admin/admin)"
}

stop_databases() {
    echo -e "${GREEN}Stopping databases...${NC}"
    docker compose down
}

run_benchmarks() {
    echo -e "${GREEN}Running benchmarks...${NC}"
    echo -e "${YELLOW}Target: 10,000 DERs @ 1s interval = 36M datapoints/hour${NC}\n"

    # Check if databases are running
    if ! docker inspect victoriametrics > /dev/null 2>&1; then
        echo -e "${YELLOW}Databases not running. Starting them first...${NC}"
        start_databases
        echo ""
    fi

    # Run benchmark with Python directly (using uv)
    cd benchmark

    if command -v uv &> /dev/null; then
        echo -e "${GREEN}Running with uv...${NC}"
        uv run python run_benchmarks.py
    else
        echo -e "${GREEN}Running with pip...${NC}"
        python3 -m pip install -q -r requirements.txt
        python3 run_benchmarks.py
    fi
}

run_quick_test() {
    echo -e "${GREEN}Running quick connectivity test...${NC}"

    echo -e "\n${BLUE}VictoriaMetrics:${NC}"
    curl -s http://localhost:8428/api/v1/status/tsdb | head -c 200
    echo -e "\n"

    echo -e "${BLUE}QuestDB:${NC}"
    curl -s "http://localhost:9000/exec?query=SELECT%20now()" | head -c 200
    echo -e "\n"

    echo -e "${BLUE}ClickHouse:${NC}"
    curl -s "http://localhost:8123/?query=SELECT%20version()"
    echo -e "\n"

    echo -e "${BLUE}TimescaleDB:${NC}"
    docker exec timescaledb psql -U postgres -c "SELECT version();" 2>/dev/null | head -3
}

clean_data() {
    echo -e "${RED}Removing all data volumes...${NC}"
    docker compose down -v
    echo -e "${GREEN}Done.${NC}"
}

show_logs() {
    docker compose logs -f "$@"
}

show_status() {
    echo -e "${GREEN}Container Status:${NC}"
    docker compose ps
}

print_header
check_docker

case "${1:-help}" in
    up|start)
        start_databases
        ;;
    down|stop)
        stop_databases
        ;;
    test|benchmark)
        run_benchmarks
        ;;
    quick)
        run_quick_test
        ;;
    clean)
        clean_data
        ;;
    logs)
        shift
        show_logs "$@"
        ;;
    status|ps)
        show_status
        ;;
    *)
        echo "Usage: $0 {up|down|test|quick|clean|logs|status}"
        echo ""
        echo "Commands:"
        echo "  up        Start all databases and Grafana"
        echo "  down      Stop all containers"
        echo "  test      Run the full benchmark suite"
        echo "  quick     Quick connectivity test"
        echo "  clean     Remove all containers and data"
        echo "  logs      Follow container logs (add service name to filter)"
        echo "  status    Show container status"
        echo ""
        echo "Example:"
        echo "  $0 up           # Start databases"
        echo "  $0 test         # Run benchmarks"
        echo "  $0 logs clickhouse  # View ClickHouse logs"
        exit 1
        ;;
esac
