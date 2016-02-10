# mesos-runonce
Mesos framework to run a command once.

# Usage
Something like the following has been working:

        ./mesos-runonce -logtostderr=true \
                -master <mesos host>:5050 \
                -address <routable IP address of localhost> \
                -v 1 \
                -task-count 1 \
                -docker-cmd "env;sleep 20;ls" \
                -docker-image alpine \
                -cpus 2 \
                -mem 2
