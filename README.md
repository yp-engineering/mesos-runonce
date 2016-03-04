# mesos-runonce
Mesos framework to run a command once.

# Usage
Something like the following has been working:

        ./mesos-runonce -master <mesos host and port> \
                -address <routable IP address of localhost> \
                -principal mesos_user \
                -secret-file <(printf password) \
                -docker-cmd "env;sleep 20;ls" \
                -docker-image alpine \
                -cpus 2 \
                -mem 2
