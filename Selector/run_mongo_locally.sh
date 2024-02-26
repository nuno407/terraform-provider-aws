# Helper script to download and run a local instance of MongoDB

if ! [ -x "$(command -v mongod)" ];
then
    # THE INSTALL SCRIPT IS SPECIFIC TO UBUNTU 22.04, 20.04 and MAC

    # Check if is mac
    if [ "$(uname -s)" == "Darwin" ]; then
        brew install mongodb-community@7.0
        exit
    # Check if is ubuntu 22.04
    elif [ "$(lsb_release -cs)" = "jammy" ]; then
        echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
    # Check if is ubuntu 20.04
    elif ["$(lsb_release -cs)" = "focal" ]; then
        echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
    else
        echo "Operating system not supported by this script, please refer to https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-ubuntu/ for specific instructions on how to install mongodb"
        exit 1

    echo "MongoDB is not installed. Installing MongoDB..."
    # Install MongoDB
    sudo apt-get install gnupg curl
    curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
        sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor

    sudo apt-get update
    sudo apt-get install -y mongodb-org=7.0.5 mongodb-org-database=7.0.5 mongodb-org-server=7.0.5 mongodb-mongosh=7.0.5 mongodb-org-mongos=7.0.5 mongodb-org-tools=7.0.5

fi
    rm -f /tmp/mongodb.log

    directory="/tmp/mongodb"
    # Emtpy mongo data directory
    if [ ! -d "$directory" ]; then
        mkdir -p "$directory"
    else
        rm -r "$directory"/*
    fi

    mongod --dbpath $directory --logpath /tmp/mongodb.log --fork
    echo "MongoDB is running on port 27017"
    echo "To stop MongoDB, run: killall mongod"
