# Helper script to download and run a local instance of MongoDB

if ! [ -x "$(command -v mongod)" ];
then
    # THE INSTALL SCRIPT IS SPECIFIC TO UBUNTU 22.04, IT MIGHT NOT WORK FOR OTHER VERSIONS
    # Refer to the official docs if it does not work https://www.mongodb.com/docs/v7.0/tutorial/install-mongodb-on-ubuntu/#std-label-install-mdb-community-ubuntu

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
