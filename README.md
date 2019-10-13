# hedis-cluster-test

This is a project for testing the clustering implementation of the Hedis library. It assumes that you have a redis cluster with three master nodes available at 127.0.0.1:<7001,7002,7003>. This can easily be achieved using the [Docker Redis Cluster](https://github.com/Grokzen/docker-redis-cluster) project. Clone that, start docker, and run `make up` and you'll be ready to go.

You'll also need to add your source checkout of the hedis `cluster` branch to `stack.yaml`. That is:
- Clone `https://bitbucket.org/juspay/hedis/pull-requests/1/wip-first-working-implementation-of-move/diff`
- Checkout the `cluster` branch of `hedis`
- Open `stack.yaml` in this project and replace `<path to hedis repo>` with the path to the checked out source 

Currently the `main` function of this project does this:

- Set the key "somekey" to the value "someval"
- Migrate the hashslot for "somekey" to a different node
- Using the same connection check that `GET SOMEKEY` returns `"someval"` both during and after the migration
