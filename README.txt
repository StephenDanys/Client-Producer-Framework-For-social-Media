Μέλη ομάδας: Γεώργιος Στέφανος Μεϊδάνης
Μαρία -Αργυρώ Κατινάκη
Αθανάσιος Μακρής

Οδηγίες για compilation
1) Τρέχουμε τις brokerMain με 1 όρισμα στην καθεμία (επιλέξτε μεταξύ 101 102 103)
Αυτά τα νούμερα λειτουργούν σαν ports
2) Τρέχουμε την PublisherMain 1 ορισμα στην καθε μια ( value> 110)
3) Τρέχουμε την Consumer Main με ένα όρισμα. Το ορισμα αυτό λειτουργεί σαν το brokerPort στο οποίο θα απευθύνεται αρχικά ο client

About: 
A framework for a social media app for streaming or dowloading videos. Broker Nodes work as the Servers for the clients. The Client requests a broker for a specific
topic, that the broker maps to the proper Publisher. The publisher then sends the video to the client.
