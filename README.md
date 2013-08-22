# pigsty-mysql 

This is a mysql output plugin for Pigsty (see https://github.com/threatstack/pigsty). 

## Installation

### Normal

npm install pigsty-mysql -g

### Latest source code from repo

npm install https://github.com/threatstack/pigsty-mysql/tarball/master -g

## Configuration 

Please add the following configuration to the `output` section in `/etc/pigsty/pigsty.config.js`
```
output: {
  
     mysql: {
       user: 'mysql-username',
       password: 'password',
       host: '127.0.0.1',
       database: 'snorby',
      // max_pool_size: 5   // optional: # of sql connections;
      // localtime: true    // optional: set to true to disable inserting into UTC. pigsty inserts event in UTC by default.
     }
} 
```

### Issues 

#### Known Issues

* tcp options table not being inserted

#### Reporting

Use the git issues, or send an email to support@threatstack.com

## License

Copyright (C) 2013 Threat Stack, Inc (https://www.threatstack.com)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.







