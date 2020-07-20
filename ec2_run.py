#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import division, print_function, with_statement

import codecs
import hashlib
import itertools
import logging
import os
import os.path
import pipes
import random
import shutil
import string
from stat import S_IRUSR
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import time
import warnings
from datetime import datetime
from optparse import OptionParser
from sys import stderr
import boto
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType, EBSBlockDeviceType
from boto import ec2

if sys.version < "3":
    from urllib2 import urlopen, Request, HTTPError
else:
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError
    raw_input = input
    xrange = range

EC2_RUN_VERSION = "0.1"
EC2_RUN_DIR = os.path.dirname(os.path.realpath(__file__))

# Default location to get the spark-ec2 scripts (and ami-list) from
DEFAULT_EC2_RUN_GITHUB_REPO = "https://github.com/jinliangwei/ec2-run.git"
DEFAULT_EC2_RUN_BRANCH = "main"

class UsageError(Exception):
    pass

# Configure and parse our command-line arguments
def parse_args():
    parser = OptionParser(
        prog="ec2-run",
        version="%prog {v}".format(v=EC2_RUN_VERSION),
        usage="%prog [options] <action> <cluster_name>\n\n"
        + "<action> can be: start, run, stop")

    parser.add_option(
        "-n", "--num-instances", type="int", default=1,
        help="The number of instances to launch. Omitted if scripts-dir is specified.")
    parser.add_option(
        "-s", "--scripts-dir",
        help="This directory contains a list of scripts to run, each on a separate ec2 instances.")
    parser.add_option(
        "-d", "--deploy-dir",
        help="The directory path on an ec2 instance under which the run script will be copied into.")
    parser.add_option(
        "-c", "--command-to-run",
        help="The command to be executed on each ec2 instance.")
    parser.add_option(
        "-o", "--output-path",
        help="Each running instance of the script may generate one output file or directory on its ec2 instance. " +
        "Specify the absolute path to that output if you need to download that output.")
    parser.add_option(
        "-l", "--output-dir",
        help="The local directory to store the output from each ec2 instance. ")
    parser.add_option(
        "-k", "--key-pair",
        help="Key pair to use on instances")
    parser.add_option(
        "-i", "--identity-file",
        help="SSH private key file to use for logging into instances")
    parser.add_option(
        "-t", "--instance-type", default="m5.large",
        help="Type of instance to launch (default: %default). " +
             "WARNING: must be 64-bit; small instances won't work")
    parser.add_option(
        "-r", "--region", default="us-east-2",
        help="EC2 region used to launch instances in, or to find them in (default: %default)")
    parser.add_option(
        "-z", "--zone", default="",
        help="Availability zone to launch instances in, or 'all' to spread " +
             "subordinates across multiple (an additional $0.01/Gb for bandwidth" +
             "between zones applies) (default: a single zone chosen at random)")
    parser.add_option(
        "-a", "--ami",
        help="Amazon Machine Image ID to use")
    parser.add_option(
        "--ebs-vol-size", metavar="SIZE", type="int", default=200,
        help="Size (in GB) of each EBS volume.")
    parser.add_option(
        "--ebs-vol-type", default="gp2",
        help="EBS volume type (e.g. 'gp2', 'standard').")
    parser.add_option(
        "--ebs-vol-num", type="int", default=1,
        help="Number of EBS volumes to attach to each node as /vol[x]. " +
             "Only support up to 8 EBS volumes.")
    parser.add_option(
        "--placement-group", type="string", default=None,
        help="Which placement group to try and launch " +
             "instances into. Assumes placement group is already " +
             "created.")
    parser.add_option(
        "--spot-price", metavar="PRICE", type="float",
        help="If specified, launch subordinates as spot instances with the given " +
             "maximum price (in dollars)")
    parser.add_option(
        "-u", "--user", default="ubuntu",
        help="The SSH user you want to connect as (default: %default)")
    parser.add_option(
        "--delete-groups", action="store_true", default=False,
        help="When destroying a cluster, delete the security groups that were created")
    parser.add_option(
        "--authorized-address", type="string", default="0.0.0.0/0",
        help="Address to authorize on created security groups (default: %default)")
    parser.add_option(
        "--additional-security-group", type="string", default="",
        help="Additional security group to place the machines in")
    parser.add_option(
        "--additional-tags", type="string", default="",
        help="Additional tags to set on the machines; tags are comma-separated, while name and " +
             "value are colon separated; ex: \"Task:MySparkProject,Env:production\"")
    parser.add_option(
        "--subnet-id", default=None,
        help="VPC subnet to launch instances in")
    parser.add_option(
        "--vpc-id", default=None,
        help="VPC to launch instances in")
    parser.add_option(
        "--private-ips", action="store_true", default=False,
        help="Use private IPs for instances rather than public if VPC/subnet " +
             "requires that.")
    parser.add_option(
        "--instance-initiated-shutdown-behavior", default="terminate",
        choices=["stop", "terminate"],
        help="Whether instances should terminate when shut down or just stop")
    parser.add_option(
        "--instance-profile-name", default=None,
        help="IAM profile name to launch instances under")

    (opts, args) = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        sys.exit(1)
    (action, cluster_name) = args

    # Boto config check
    # http://boto.cloudhackers.com/en/latest/boto_config_tut.html
    home_dir = os.getenv('HOME')
    if home_dir is None or not os.path.isfile(home_dir + '/.boto'):
        if not os.path.isfile('/etc/boto.cfg'):
            # If there is no boto config, check aws credentials
            if not os.path.isfile(home_dir + '/.aws/credentials'):
                if os.getenv('AWS_ACCESS_KEY_ID') is None:
                    print("ERROR: The environment variable AWS_ACCESS_KEY_ID must be set",
                          file=stderr)
                    sys.exit(1)
                if os.getenv('AWS_SECRET_ACCESS_KEY') is None:
                    print("ERROR: The environment variable AWS_SECRET_ACCESS_KEY must be set",
                          file=stderr)
                    sys.exit(1)
    return (opts, action, cluster_name)

# Get the EC2 security group of the given name, creating it if it doesn't exist
def get_or_make_group(conn, name, vpc_id):
    groups = conn.get_all_security_groups()
    group = [g for g in groups if g.name == name]
    if len(group) > 0:
        return group[0]
    else:
        print("Creating security group " + name)
        return conn.create_security_group(name, "EC2 Run group", vpc_id)

# Source: http://aws.amazon.com/amazon-linux-ami/instance-type-matrix/
# Last Updated: 2018-04-29
# For easy maintainability, please keep this manually-inputted dictionary sorted by key.
# This is an incomplete list.
EC2_INSTANCE_TYPES = {
    "c4.large":    "hvm",
    "c4.xlarge":   "hvm",
    "c4.2xlarge":  "hvm",
    "c4.4xlarge":  "hvm",
    "c4.8xlarge":  "hvm",
    "c5.large":    "hvm",
    "c5.xlarge":   "hvm",
    "c5.2xlarge":  "hvm",
    "c5.4xlarge":  "hvm",
    "c5.9xlarge":  "hvm",
    "c5.18xlarge": "hvm",
    "g3.4xlarge":   "hvm",
    "g3.8xlarge":  "hvm",
    "g3.16xlarge": "hvm",
    "m4.large":    "hvm",
    "m4.xlarge":   "hvm",
    "m4.2xlarge":  "hvm",
    "m4.4xlarge":  "hvm",
    "m4.10xlarge": "hvm",
    "m4.16xlarge": "hvm",
    "m5.large":    "hvm",
    "m5.xlarge":   "hvm",
    "m5.2xlarge":  "hvm",
    "m5.4xlarge":  "hvm",
    "m5.12xlarge": "hvm",
    "m5.24xlarge": "hvm",
    "r4.large":    "hvm",
    "r4.xlarge":   "hvm",
    "r4.2xlarge":  "hvm",
    "r4.4xlarge":  "hvm",
    "r4.8xlarge":  "hvm",
    "r4.16xlarge": "hvm",
    "p2.large":    "hvm",
    "p2.8xlarge":  "hvm",
    "p2.16xlarge": "hvm"
}

# Launch a cluster of the given name, by setting up its security groups,
# and then starting new instances in them.
# Returns a tuple of EC2 reservation objects for the subordinates
# Fails if there already instances running in the cluster's groups.
def launch_cluster(conn, opts, num_nodes, cluster_name):
    if opts.identity_file is None:
        print("ERROR: Must provide an identity file (-i) for ssh connections.", file=stderr)
        sys.exit(1)

    if opts.key_pair is None:
        print("ERROR: Must provide a key pair name (-k) to use on instances.", file=stderr)
        sys.exit(1)

    print("Setting up security groups...")

    subordinate_group = get_or_make_group(conn, cluster_name + "-subordinates", opts.vpc_id)
    authorized_address = opts.authorized_address
    if subordinate_group.rules == []:  # Group was just now created
        if opts.vpc_id is None:
            subordinate_group.authorize(src_group=subordinate_group)
        else:
            subordinate_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                  src_group=subordinate_group)
            subordinate_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                  src_group=subordinate_group)
            subordinate_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                  src_group=subordinate_group)
        subordinate_group.authorize('tcp', 22, 22, authorized_address)

    # Check if instances are already running in our groups
    existing_subordinates = get_existing_cluster(conn, opts, cluster_name, die_on_error=False)
    if existing_subordinates:
        print("ERROR: There are already instances running in group %s" %
              subordinate_group.name, file=stderr)
        sys.exit(1)

    if opts.ami is None:
        print("ERROR: AMI is not set, exit")
        sys.exit(1)

    # we use group ids to work around https://github.com/boto/boto/issues/350
    additional_group_ids = []
    if opts.additional_security_group:
        additional_group_ids = [sg.id
                                for sg in conn.get_all_security_groups()
                                if opts.additional_security_group in (sg.name, sg.id)]
    print("Launching instances...")

    try:
        image = conn.get_all_images(image_ids=[opts.ami])[0]
    except:
        print("Could not find AMI " + opts.ami, file=stderr)
        sys.exit(1)

    # Create block device mapping so that we can add EBS volumes if asked to.
    # The first drive is attached as /dev/sds, 2nd as /dev/sdt, ... /dev/sdz
    block_map = BlockDeviceMapping()
    if opts.ebs_vol_size > 0:
        for i in range(opts.ebs_vol_num):
            device = EBSBlockDeviceType()
            device.size = opts.ebs_vol_size
            device.volume_type = opts.ebs_vol_type
            device.delete_on_termination = True
            block_map["/dev/sd" + chr(ord('s') + i)] = device

    # Launch subordinates
    if opts.spot_price is not None:
        # Launch spot instances with the requested price
        print("Requesting %d subordinates as spot instances with price $%.3f" %
              (num_nodes, opts.spot_price))
        zones = get_zones(conn, opts)
        num_zones = len(zones)
        i = 0
        my_req_ids = []
        for zone in zones:
            num_subordinates_this_zone = get_partition(num_nodes, num_zones, i)
            subordinate_reqs = conn.request_spot_instances(
                price=opts.spot_price,
                image_id=opts.ami,
                launch_group="launch-group-%s" % cluster_name,
                placement=zone,
                count=num_subordinates_this_zone,
                key_name=opts.key_pair,
                security_group_ids=[subordinate_group.id] + additional_group_ids,
                instance_type=opts.instance_type,
                block_device_map=block_map,
                subnet_id=opts.subnet_id,
                placement_group=opts.placement_group,
                instance_profile_name=opts.instance_profile_name)
            my_req_ids += [req.id for req in subordinate_reqs]
            i += 1

        print("Waiting for spot instances to be granted...")
        try:
            while True:
                time.sleep(10)
                reqs = conn.get_all_spot_instance_requests()
                id_to_req = {}
                for r in reqs:
                    id_to_req[r.id] = r
                active_instance_ids = []
                for i in my_req_ids:
                    if i in id_to_req and id_to_req[i].state == "active":
                        active_instance_ids.append(id_to_req[i].instance_id)
                if len(active_instance_ids) == num_nodes:
                    print("All %d spot instances granted" % (num_nodes + 1))
                    reservations = conn.get_all_reservations(active_instance_ids)
                    subordinate_nodes = []
                    for r in reservations:
                        subordinate_nodes += r.instances
                    break
                else:
                    print("%d of %d subordinate spot instances granted, waiting longer" % (
                            len(active_instance_ids), num_nodes))
        except:
            print("Canceling spot instance requests")
            conn.cancel_spot_instance_requests(my_req_ids)
            # Log a warning if any of these requests actually launched instances:
            subordinate_nodes = get_existing_cluster(conn, opts, cluster_name, die_on_error=False)
            running = len(subordinate_nodes)
            if running:
                print(("WARNING: %d instances are still running" % running), file=stderr)
            sys.exit(0)
    else:
        print ("WARNING: --spot-price was not set; consider launch subordinates as spot instances to save money")
        # Launch non-spot instances
        zones = get_zones(conn, opts)
        num_zones = len(zones)
        i = 0
        subordinate_nodes = []
        for zone in zones:
            num_subordinates_this_zone = get_partition(num_nodes, num_zones, i)
            if num_subordinates_this_zone > 0:
                subordinate_res = image.run(
                    key_name=opts.key_pair,
                    security_group_ids=[subordinate_group.id] + additional_group_ids,
                    instance_type=opts.instance_type,
                    placement=zone,
                    min_count=num_subordinates_this_zone,
                    max_count=num_subordinates_this_zone,
                    block_device_map=block_map,
                    subnet_id=opts.subnet_id,
                    placement_group=opts.placement_group,
                    instance_initiated_shutdown_behavior=opts.instance_initiated_shutdown_behavior,
                    instance_profile_name=opts.instance_profile_name)
                subordinate_nodes += subordinate_res.instances
                print("Launched {s} subordinate{plural_s} in {z}, regid = {r}".format(
                      s=num_subordinates_this_zone,
                      plural_s=('' if num_subordinates_this_zone == 1 else 's'),
                      z=zone,
                      r=subordinate_res.id))
            i += 1


    print("Waiting for AWS to propagate instance metadata...")
    time.sleep(15)

    # Give the instances descriptive names and set additional tags
    additional_tags = {}
    if opts.additional_tags.strip():
        additional_tags = dict(
            map(str.strip, tag.split(':', 1)) for tag in opts.additional_tags.split(',')
        )

    for subordinate in subordinate_nodes:
        subordinate.add_tags(
            dict(additional_tags, Name='{cn}-subordinate-{iid}'.format(cn=cluster_name, iid=subordinate.id))
        )

    # Return all the instances
    return subordinate_nodes


def get_existing_cluster(conn, opts, cluster_name, die_on_error=True):
    """
    Get the EC2 instances in an existing cluster if available.
    Returns a tuple of lists of EC2 instance objects for the mains and subordinates.
    """
    print("Searching for existing cluster {c} in region {r}...".format(
          c=cluster_name, r=opts.region))

    def get_instances(group_names):
        """
        Get all non-terminated instances that belong to any of the provided security groups.

        EC2 reservation filters and instance states are documented here:
            http://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html#options
        """
        reservations = conn.get_all_reservations(
            filters={"instance.group-name": group_names})
        instances = itertools.chain.from_iterable(r.instances for r in reservations)
        return [i for i in instances if i.state not in ["shutting-down", "terminated"]]

    subordinate_instances = get_instances([cluster_name + "-subordinates"])

    if any(subordinate_instances):
        print("Found {s} subordinate{plural_s}.".format(
              s=len(subordinate_instances),
              plural_s=('' if len(subordinate_instances) == 1 else 's')))
    return subordinate_instances

def permit_root_ssh_login(host, opts):
    tries = 0
    cmd = "sudo cp ~/.ssh/authorized_keys /root/.ssh/authorized_keys"
    while True:
        try:
            return subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', '%s@%s' % ("ubuntu", host),
                                     stringify_command(cmd)])
        except subprocess.CalledProcessError as e:
            if tries > 5:
                # If this was an ssh failure, provide the user with hints.
                if e.returncode == 255:
                    raise UsageError(
                        "Failed to SSH to remote host {0}.\n"
                        "Please check that you have provided the correct --identity-file and "
                        "--key-pair parameters and try again.".format(host))
                else:
                    raise e
            print("Error executing remote command, retrying after 30 seconds: {0}".format(e),
                  file=stderr)
            time.sleep(30)
            tries = tries + 1

def is_ssh_available(host, opts, print_ssh_output=True):
    """
    Check if SSH is available on a host.
    """
    s = subprocess.Popen(
        ssh_command(opts) + ['-t', '-t', '-o', 'ConnectTimeout=3',
                             '%s@%s' % (opts.user, host), stringify_command('true')],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT  # we pipe stderr through stdout to preserve output order
    )
    cmd_output = s.communicate()[0]  # [1] is stderr, which we redirected to stdout

    if s.returncode != 0 and print_ssh_output:
        # extra leading newline is for spacing in wait_for_cluster_state()
        print(textwrap.dedent("""\n
            Warning: SSH connection error. (This could be temporary.)
            Host: {h}
            SSH return code: {r}
            SSH output: {o}
        """).format(
            h=host,
            r=s.returncode,
            o=cmd_output.strip()
        ))

    return s.returncode == 0


def is_cluster_ssh_available(cluster_instances, opts):
    """
    Check if SSH is available on all the instances in a cluster.
    """
    for i in cluster_instances:
        dns_name = get_dns_name(i, opts.private_ips)
        if not is_ssh_available(host=dns_name, opts=opts):
            return False
    else:
        return True


def wait_for_cluster_state(conn, opts, cluster_instances, cluster_state):
    """
    Wait for all the instances in the cluster to reach a designated state.

    cluster_instances: a list of boto.ec2.instance.Instance
    cluster_state: a string representing the desired state of all the instances in the cluster
           value can be 'ssh-ready' or a valid value from boto.ec2.instance.InstanceState such as
           'running', 'terminated', etc.
           (would be nice to replace this with a proper enum: http://stackoverflow.com/a/1695250)
    """
    sys.stdout.write(
        "Waiting for cluster to enter '{s}' state.".format(s=cluster_state)
    )
    sys.stdout.flush()

    start_time = datetime.now()
    num_attempts = 0

    while True:
        time.sleep(5 * num_attempts)  # seconds

        for i in cluster_instances:
            i.update()

        max_batch = 100
        statuses = []
        for j in xrange(0, len(cluster_instances), max_batch):
            batch = [i.id for i in cluster_instances[j:j + max_batch]]
            statuses.extend(conn.get_all_instance_status(instance_ids=batch))

        if cluster_state == 'ssh-ready':
            if all(i.state == 'running' for i in cluster_instances) and \
               all(s.system_status.status == 'ok' for s in statuses) and \
               all(s.instance_status.status == 'ok' for s in statuses) and \
               is_cluster_ssh_available(cluster_instances, opts):
                break
        else:
            if all(i.state == cluster_state for i in cluster_instances):
                break

        num_attempts += 1

        sys.stdout.write(".")
        sys.stdout.flush()

    sys.stdout.write("\n")

    end_time = datetime.now()
    print("Cluster is now in '{s}' state. Waited {t} seconds.".format(
        s=cluster_state,
        t=(end_time - start_time).seconds
    ))


def stringify_command(parts):
    if isinstance(parts, str):
        return parts
    else:
        return ' '.join(map(pipes.quote, parts))


def ssh_args(opts):
    parts = ['-o', 'StrictHostKeyChecking=no']
    parts += ['-o', 'UserKnownHostsFile=/dev/null']
    if opts.identity_file is not None:
        parts += ['-i', opts.identity_file]
    return parts

def ssh_command(opts):
    return ['ssh'] + ssh_args(opts)

# Run a command on a host through ssh, retrying up to five times
# and then throwing an exception if ssh continues to fail.
def ssh(host, opts, command):
    tries = 0
    while True:
        try:
            return subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', '%s@%s' % (opts.user, host),
                                     stringify_command(command)])
        except subprocess.CalledProcessError as e:
            if tries > 5:
                # If this was an ssh failure, provide the user with hints.
                if e.returncode == 255:
                    raise UsageError(
                        "Failed to SSH to remote host {0}.\n"
                        "Please check that you have provided the correct --identity-file and "
                        "--key-pair parameters and try again.".format(host))
                else:
                    raise e
            print("Error executing remote command, retrying after 30 seconds: {0}".format(e),
                  file=stderr)
            time.sleep(30)
            tries = tries + 1

# Gets a list of zones to launch instances in
def get_zones(conn, opts):
    if opts.zone == 'all':
        zones = [z.name for z in conn.get_all_zones()]
    else:
        zones = [opts.zone]
    return zones

# Gets the number of items in a partition
def get_partition(total, num_partitions, current_partitions):
    num_subordinates_this_zone = total // num_partitions
    if (total % num_partitions) - current_partitions > 0:
        num_subordinates_this_zone += 1
    return num_subordinates_this_zone

# Gets the DNS name, taking into account the --private-ips flag
def get_dns_name(instance, private_ips=False):
    dns = instance.public_dns_name if not private_ips else \
        instance.private_ip_address
    if not dns:
        raise UsageError("Failed to determine hostname of {0}.\n"
                         "Please check that you provided --private-ips if "
                         "necessary".format(instance))
    return dns

def get_scripts_to_run(scripts_dir):
    return [fname for fname in os.listdir(scripts_dir) if os.path.isfile(os.path.join(scripts_dir, fname))]

def upload_scripts(subordinate_nodes, script_list, opts):
    for idx in range(0, len(script_list)):
        script_fname = script_list[idx]
        subordinate_node = subordinate_nodes[idx]
        fname_path = os.path.join(opts.scripts_dir, script_fname)
        subordinate_name = get_dns_name(subordinate_node, opts.private_ips)
        command = [
            'rsync', '-v',
            '-e', stringify_command(ssh_command(opts)),
            fname_path,
            '%s@%s:%s' % (opts.user, subordinate_name, opts.deploy_dir)
            ]
        print(command)
        subprocess.check_call(command)

def download_results(subordinate_nodes, script_list, exit_codes, opts):
    for idx in range(0, len(exit_codes)):
        if exit_codes[idx] != 0:
            continue
        subordinate_node = subordinate_nodes[idx]
        script_name = script_list[idx]
        subordinate_name = get_dns_name(subordinate_node, opts.private_ips)
        fname_path = os.path.join(opts.output_dir, ("output." + script_name + "." + subordinate_name))

        command = [
            'rsync', '-rv',
            '-e', stringify_command(ssh_command(opts)),
            '%s@%s:%s' % (opts.user, subordinate_name, opts.output_path),
            fname_path
            ]
        print(command)
        subprocess.check_call(command)


def run_and_wait_until_finish(subordinate_nodes, script_list, opts):
    worker_proc_list = []
    for idx in range(0, len(script_list)):
        script_fname = script_list[idx]
        subordinate_node = subordinate_nodes[idx]
        command_to_run = opts.command_to_run % script_fname
        print(command_to_run)
        subordinate_name = get_dns_name(subordinate_node, opts.private_ips)
        worker_proc = subprocess.Popen(
            ssh_command(opts) + ['%s@%s' % (opts.user, subordinate_name),
                                 command_to_run])
        worker_proc_list.append(worker_proc)
    exit_codes = [p.wait() for p in worker_proc_list]
    print(exit_codes)
    num_succeeded_runs = sum([1 if ec == 0 else 0 for ec in exit_codes])
    print("%d of %d runs are successful" % (num_succeeded_runs, len(exit_codes)))
    if opts.output_dir != None and opts.output_path != None:
        download_results(subordinate_nodes, script_list, exit_codes, opts)
    print("results downloaded")

def real_main():
    (opts, action, cluster_name) = parse_args()
    if opts.scripts_dir is not None:
        script_list = get_scripts_to_run(opts.scripts_dir)
        num_nodes = len(script_list)
    else:
        script_list = []
        num_nodes = opts.num_instances
    print(script_list)
    if opts.identity_file is not None:
        if not os.path.exists(opts.identity_file):
            print("ERROR: The identity file '{f}' doesn't exist.".format(f=opts.identity_file),
                  file=stderr)
            sys.exit(1)

        file_mode = os.stat(opts.identity_file).st_mode
        if not (file_mode & S_IRUSR) or not oct(file_mode)[-2:] == '00':
            print("ERROR: The identity file must be accessible only by you.", file=stderr)
            print('You can fix this with: chmod 400 "{f}"'.format(f=opts.identity_file),
                  file=stderr)
            sys.exit(1)

    if opts.instance_type not in EC2_INSTANCE_TYPES:
        print("Warning: Unrecognized EC2 instance type for instance-type: {t} -- right now we are only supporting a subset of the instance types".format(
              t=opts.instance_type), file=stderr)
        sys.exit(1)

    if opts.ebs_vol_num > 8:
        print("ebs-vol-num cannot be greater than 8", file=stderr)
        sys.exit(1)

    try:
        conn = ec2.connect_to_region(opts.region)
    except Exception as e:
        print((e), file=stderr)
        sys.exit(1)
    print("connect = ", conn)

    # Select an AZ at random if it was not specified.
    if opts.zone == "":
        opts.zone = random.choice(conn.get_all_zones()).name

    if action == "start":
        subordinate_nodes = launch_cluster(conn, opts, num_nodes, cluster_name)
        wait_for_cluster_state(
            conn=conn,
            opts=opts,
            cluster_instances=subordinate_nodes,
            cluster_state='ssh-ready'
        )
        upload_scripts(subordinate_nodes, script_list, opts)
        run_and_wait_until_finish(subordinate_nodes, script_list, opts)
        print("Stopping subordinates...")
        for inst in subordinate_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                if inst.spot_instance_request_id:
                    inst.terminate()
                else:
                    inst.stop()
    elif action == "run":
        subordinate_nodes = get_existing_cluster(conn, opts, cluster_name, die_on_error=False)
        upload_scripts(subordinate_nodes, script_list, opts)
        run_and_wait_until_finish(subordinate_nodes, script_list, opts)

    elif action == "stop":
        response = raw_input(
            "Are you sure you want to stop the cluster " +
            cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
            "BUT THE CLUSTER WILL KEEP USING SPACE ON\n" +
            "AMAZON EBS IF IT IS EBS-BACKED!!\n" +
            "All data on spot-instance subordinates will be lost.\n" +
            "Stop cluster " + cluster_name + " (y/N): ")
        if response == "y":
            subordinate_nodes = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            print("Stopping subordinates...")
            for inst in subordinate_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    if inst.spot_instance_request_id:
                        inst.terminate()
                    else:
                        inst.stop()
    else:
        print("Invalid action: %s" % action, file=stderr)
        sys.exit(1)


def main():
    try:
        real_main()
    except UsageError as e:
        print("\nError:\n", e, file=stderr)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig()
    main()
