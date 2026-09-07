# ReductStore Snap

ReductStore is published as a strictly confined snap named `reductstore`. Runtime settings such as `data-path`, `api-token`, `host`, and `port` are managed as direct snap options and are exported by `snap/local/scripts/bin/wrapper` when the service starts.

The `env-file-path` option can point to an additional shell environment file. The wrapper sources that file first, then exports direct snap options afterward, so direct snap options take precedence over values loaded from the environment file.

## Configuration Snap

The snap declares an optional read-only content plug named `reductstore-configuration`. A separate provider snap can use it to expose a provisioning file such as `reductstore.env` under this stable consumer path:

```console
/var/snap/reductstore/common/config/reductstore.env
```

Provider snaps must expose the same versioned content label and should use `read:` so the consumer receives configuration as read-only content:

```yaml
slots:
  reductstore-configuration:
    interface: content
    content: reductstore-configuration-v1
    read:
      - $SNAP/config
```

Connect the provider snap and point ReductStore at the mounted file:

```console
sudo snap connect reductstore:reductstore-configuration robot-reductstore-config:reductstore-configuration
sudo snap set reductstore env-file-path=/var/snap/reductstore/common/config/reductstore.env
sudo snap restart reductstore
```

The environment file is intended for provisioning variables that are not already managed directly by the wrapper. Direct snap options such as `data-path`, `api-token`, `host`, and `port` override values loaded from `reductstore.env`.

ReductStore reads the environment file at service startup. Restart the service after refreshing or changing the provider snap content:

```console
sudo snap restart reductstore
```

## Ubuntu Core Gadget

Ubuntu Core gadget defaults and connections use snap IDs rather than snap names. Seed both the ReductStore snap and the provider snap in the model, then use placeholders like this in the gadget:

```yaml
defaults:
  <reductstore-snap-id>:
    env-file-path: /var/snap/reductstore/common/config/reductstore.env

connections:
  - plug: <reductstore-snap-id>:reductstore-configuration
    slot: <provider-config-snap-id>:reductstore-configuration
```

## Manual Validation

Build or install `reductstore` and a minimal provider snap with a matching `reductstore-configuration-v1` content slot. Put a valid `reductstore.env` file in the provider's exposed `$SNAP/config` directory, connect the interface, set `env-file-path` to `/var/snap/reductstore/common/config/reductstore.env`, restart the service, and confirm ReductStore starts with the supplied provisioning variables.

Installing and running `reductstore` without a connected provider remains unchanged.
