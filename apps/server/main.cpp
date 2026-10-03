#include <userver/components/minimal_server_component_list.hpp>
#include <userver/storages/postgres/component.hpp>
#include <userver/ugrpc/server/component_list.hpp>
#include <userver/utils/daemon_run.hpp>

int main(int argc, char* argv[]) {
    const auto components = userver::components::MinimalServerComponentList()
        .Append<userver::components::Postgres>("upload-database")
        .AppendComponentList(userver::ugrpc::server::MinimalComponentList());
    return userver::utils::DaemonMain(argc, argv, components);
}
