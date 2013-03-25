#!/usr/bin/env ruby
require 'rubygems'
require 'aws-sdk'

AWS.config(
  :access_key_id => ENV['AMAZON_ACCESS_KEY_ID'],
  :secret_access_key => ENV['AMAZON_SECRET_ACCESS_KEY']
)
@dynamo=AWS::DynamoDB.new
puts 'Deleting any old tables (may take some time)'
@dynamo.tables.each do |table|
  table.delete
  sleep 1 while table.exists? 
end
puts 'Old tables deleted.'
table1=@dynamo.tables.create(
  "Table1",3,3,
  :hash_key => { :zipcode => :string }
)
puts 'Creating table1... (may take some time)'
sleep 1 while table1.status == :creating
puts "Status: #{table1.status} "
puts 'Table1 created!'
table2=@dynamo.tables.create(
  "Table2",5,5,
  :hash_key => { :city => :string },
  :range_key => { :zipcode => :string }
)
puts 'Creating table2... (may take some time)'
sleep 1 while table2.status == :creating
puts "Status: #{table2.status} "
puts 'Table2 created!'

puts
puts 'All tables associated with this account (by name)'
@dynamo.tables.each do |table|
  puts table.name
end

puts
puts 'Description of each table'
@dynamo.tables.each do |table|
  puts "Table name: #{table.name}"
  puts "Status: #{table.status}"
  puts "Table Hash and Range keys: #{table.hash_key}, #{table.range_key}"
  puts "Date of table creation: #{table.creation_date_time}"
  puts
end

table1.load_schema
puts 'Populating table 1'
file = File.open('zipcodes.txt')
20.times do
  line = file.readline.chomp!
  line = line.delete('"').split(',')
  table1.items.create(
  :zipcode => line[0],
  :latitude => line[1].to_f,
  :longitude => line[2].to_f,
  :city => line[3],
  :state => line[4],
  :county => line[5],
  :type => line[6]
  )
end
puts 'Table1 has been populated.'

table2.load_schema
puts 'Populating Table2'
file = File.open('zipcodes.txt')
40.times do
  line = file.readline
  line = line.delete('"').split(',')
  table2.items.create(
  :zipcode => line[0],
  :latitude => line[1].to_f,
  :longitude => line[2].to_f,
  :city => line[3],
  :state => line[4],
  :county => line[5],
  :type => line[6]
  )
end
puts 'Table2 has been populated.'

findZip='00610'
puts "Finding zipcodes greater than #{findZip}"
table1.items.where(:zipcode).greater_than(findZip).each do |zip|
  puts zip.attributes.each_value
  puts
end

findCity='ARECIBO'
puts "Finding all lines which have the city #{findCity}"
table2.items.where(:city).equals(findCity).each do |zip|
  puts zip.attributes.each_value
  puts
end
puts "Finding all lines which have a city name greater than #{findCity}"
table2.items.where(:city).greater_than(findCity).each do |city|
  puts city.attributes.each_value
  puts ''
end

action=''
while(action!='exit')
  while(true)
    puts 'Choose a table (1 or 2)'
    choice=gets
    choice.chomp!
    if(choice=='1')
      tableChoice=table1
      break
    elsif(choice=='2')
      tableChoice=table2
      break
    else
      puts 'Choice not recognized.'
    end
  end
  puts 'What action do you want to take?'
  action = gets
  action.chomp!
  if(action=='delete')
    puts 'What item do you want to delete?'
    item=gets
    item.chomp!
    itemOb=tableChoice.items.where(:zipcode).equals(item)
    if(itemOb.count>0)
      puts "Item #{item} found. Information displayed below."
      itemOb.each do |i|
        puts i.attributes.each_value
      end
      puts "Are you sure you want to delete this item?"
      answer=gets
      answer.chomp!
      if(answer=='y')
        itemOb.each do |i|
          i.delete
        end
        puts 'Item deleted'
      else
        puts 'Operation aborted'
      end
    else
      puts 'Item not found.'
    end
  end
  if(action=='update')
    puts 'What item do you want to update?'
    item=gets
    item.chomp!
    itemOb=tableChoice.items.where(:zipcode).equals(item)
    if(itemOb.count>0)
      puts "Item #{item} found. Information displayed below."
      itemOb.each do |i|
        puts i.attributes.each_value
        puts 'What attribute do you want to change?'
        att=gets
        att.chomp!
        if(att=='zipcode')
          puts "Cannot change #{att} because it is used in the hashing process."
        elsif(choice=='2' and att=='city')
          puts "Cannot change #{att} because it is used in the hashing process."
        else
          puts "Current value of #{att} is below."
          puts i.attributes.values_at(att)
          puts "What do you want to change #{att} to?"
          newValue=gets
          newValue.chomp!
          puts "Are you sure you want to change #{att} to #{newValue} for #{item} ?"
          answer=gets
          answer.chomp!
          if(answer=='y')
            i.attributes.set(att => newValue)
            puts 'Succeeded.'
          else
            puts 'Operation aborted'
          end
        end
      end
    else
      puts 'Item not found.'
    end
  end
end
table1.provision_throughput(:read_capacity_units => 4, :write_capacity_units => 4)
table2.provision_throughput(:read_capacity_units => 6, :write_capacity_units => 6)
puts 'Choose a table to delete.'
choice = gets
choice.chomp!
if(choice=='1')
  table1.delete
  puts 'Table1 deleted.'
elsif(choice=='2')
  table2.delete
  puts 'Table2 deleted'
else
  puts 'No tables deleted.'
