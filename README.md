#DE-II project
A distributed pipeline over Apache Pulsar that ingests recent GitHub repositories,
enriches each one with commit counts and test/CI signals, and answers four
research questions:


We use a total of 5 VMs. 

We use a controller VM that starts 4 worker VMs automatically via Openstack API.

Here is a step-to-step guide which shows how to set up the architecture correctly.


## Setup

### 1. Start the controller VM on Openstack
Launch a small flavoured VM by pasting the cloud-init script in the Configuration step on Openstack: controller.yaml. The script is located in the /controller/cloud-init/controller.yaml:

   ```
   https://raw.githubusercontent.com/Theodor-Bern/DE-II-Project/main/controller/cloud-init/control$
   ```
Cloud-init runs in the background for about ~5 minutes. Don't ssh in yet.

### 2. SSH in to controller VM

### 3. Generate ssh key on controller VM

In order to launch the cluster, it is needed to generate the key. IMPORTANT: name the key: de2-key!

```bash
ssh-keygen -t rsa -b 4096 -f ~/.ssh/de2-key -N ""
chmod 600 ~/.ssh/de2-key
```


###4. Upload the public key to Openstack
In Horizon:
1. **Compute → Key Pairs → Import Public Key**
2. Paste the contents of `~/.ssh/de2-key.pub`
3. Name the key, remember the name for later steps.


###5 Download your Openstack RC file.
1. Paste the content of your RC file into a file called ~/openrc.sh

###6 Create a Github PAT
1. Go to Github and then go to settings->Developer settings-> Personal access tokens -> Tokens (classic)
2. Generate new token with scopes: puplic_repo, read:org
3. Go to your controller VM. Use nano to create the file `~/.github_tokens
4. Name one token per line: 
 ```
   GITHUB_TOKEN_1=ghp_xxxxxxxxxxxx
   GITHUB_TOKEN_2=ghp_xxxxxxxxxxxx   # optional
   GITHUB_TOKEN_3=ghp_xxxxxxxxxxxx   # optional
   GITHUB_TOKEN_4=ghp_xxxxxxxxxxxx   # optional
   GITHUB_TOKEN_5=ghp_xxxxxxxxxxxx   # optional
   ```

NOTE: YOU NEED TO NAME IT AS GITHUB_TOKEN_N OTHERWISE IT WILL NOT WORK

###7. Configure paths:
## 8. Configure paths

On the controller VM:

```bash
cd ~/de2
cp .env.example .env
nano .env
```

Fill in (paths are on the **controller VM**, not your laptop):

```
SSH_KEY_PATH=/home/ubuntu/.ssh/de2-key
OPENSTACK_KEY_NAME=de2-key
OPENRC_PATH=/home/ubuntu/openrc.sh
GITHUB_TOKENS_PATH=/home/ubuntu/.github_tokens
DAYS_BACK=3
```

`DAYS_BACK=3` runs a quick demo (~30 minutes). The default 365 takes hours
with one GitHub token.

## Run

On the controller VM:

```bash
./run.sh deploy
```

What happens:
1. You're prompted for your OpenStack password (the same one you log into
   Horizon with).
2. **Phase 1** (~2 min): broker VM is provisioned, its IP captured.
3. **Phase 2** (~3 min): producer, consumer, aggregator VMs are provisioned
   in parallel with the broker IP injected into their cloud-init.
4. **Wait for cloud-init** (~5 min): each VM installs Docker and pulls its
   service image.
5. **Distribute**: SCPs compose files and your tokens to each VM.
6. **Start services**: broker first, then aggregators + consumers, then
   producer.

Total: ~10 min. When you see `[deploy] Cluster up`, the pipeline is running.
`deploy.sh` exits — it does not block until the producer is done.

## Collect results

Wait 10–20 min so the pipeline has time to process some data, then on the
controller VM:

```bash
./run.sh collect
```

This snapshots everything under `~/de2/results/snapshot-<timestamp>/`:

You can run `./run.sh collect` repeatedly, each call writes a new snapshot
with the latest numbers.

The 5 VMs persist until you delete them manually:

1. Horizon → **Compute → Instances**
2. Select all five (controller, broker-vm-*, producer-vm-*, consumer-vm-*,
   aggregator-vm-*)
3. **Actions → Delete Instances**

