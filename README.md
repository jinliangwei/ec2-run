This simple script lets you launch a cluster of EC2 instances on AWS, upload a different script to each instance, run those scripts in parllel, download their results in the end of the run
and finally tear down the cluster.

This script depends on [Boto](http://boto.cloudhackers.com/en/latest/) for AWS API. Install Boto via
```
pip install boto
```

## Before You Start
-   Create a key pair for yourself on AWS.
-   Set the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to your
    Amazon EC2 access key ID and secret access key. These can be
    obtained from the [AWS homepage](http://aws.amazon.com/) by following
    Account > Security Credentials > Access Credentials.

## Launching a Cluster

-  Typical usage:
   `python ec2_run.py -k <key-pair-name> -i <private-key> -n <num-instnaces> -a <AMI-ID>
   -r <region> --spot-price=<upper-limit-for-spot-request-in-USD> -u <user-name>
   -t <instance-type>
   -s <local-path-to-the-directory-containing-scripts-to-run>
   -d <remote-path-under-which-the-script-will-be-uploaded>
   -c <command-to-run-on-each-instance>
   -o <path-to-the-output-file-on-each-instance>
   -l <local-path-to-contain-the-downloaded-results>
   start <cluster-name>`
   This command launches a cluster named <cluster-name>, uploads the scripts, one to each instance,
   run the command, download the results and finally tear down the cluster.
-  Run script on a existing cluster:
   If you already have a cluster named <cluster-name> running, you can upload scripts to it to run. Note that this will not
   terminate your cluster in the end! Example:
   `python ec2_run.py -k <key-pair-name> -i <private-key>
   -s <local-path-to-the-directory-containing-scripts-to-run>
   -d <remote-path-under-which-the-script-will-be-uploaded>
   -c <command-to-run-on-each-instance>
   -o <path-to-the-output-file-on-each-instance>
   -l <local-path-to-contain-the-downloaded-results>
   run <cluster-name>`
- Terminate a cluster:
   `python ec2_run.py -k <key-pair-name> -i <private-key>
    stop <cluster-name>`
- An example:
   `python ec2_run.py -k <key-pair-name> -i <private-key> -n <num-instnaces> -a <AMI-ID>
   -r <region> --spot-price=<upper-limit-for-spot-request-in-USD> -u <user-name>
   -t <instance-type>
   -s /Users/awesomeyou/ec2-run.git/test_scripts/
   -d /home/ubuntu
   -c "python /home/ubuntu/%s /home/ubuntu/testout"
   -o /home/ubuntu/testout
   -l /Users/jinliangwei/Work/ec2-run.git/test_scripts.out/
   start <cluster-name>`
