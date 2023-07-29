const TelegramBot = require( 'node-telegram-bot-api' ) 
const mysql = require('mysql');
const { Client } = require('pg')
const {resolve} = require('path');
YAML = require('yamljs');
const {tg_token, tg_group_id, csv_file} = YAML.load( resolve(__dirname, 'user_files', 'data.yml')  );
const inMinutes = 60*1000

console.log('\n', '--------------------------','\n', `-- Monitor Base запущен --`,'\n','--------------------------', '\n', '            --            ' );

const bot = new TelegramBot(tg_token, {
  polling: {
    interval: 300,
    autoStart: true,
    params: {
      timeout: 100
    }
  },
});

bot.setMyCommands(
  [
    {
      "command": "start",
      "description": "Запуск бота"
    }
  ]
)

const baseListPromice = new Promise((resolve, reject)=>{
  const csv = require('csv-parser'); 
  const fs = require('fs'); 
  const readCvs = []
  fs.createReadStream(`./${csv_file}`) 
    .pipe(csv()) 
    .on('data', async (row) => { 
      readCvs.push(row)})
    .on('end', () => {
      resolve(readCvs)
    }) 
})

async function baseMonitor(host, user, password, database, port, table, table_field, db_type){
  if(db_type == 'mysql'){
    const conn = mysql.createConnection({
      host     : host,
      user     : user,
      password : password,
      database: database,
      port: port
    });
    return new Promise((resolve, reject) => {
      const query = `SELECT ${table_field} FROM ${table} ORDER BY ${table_field} DESC LIMIT 1`
      conn.query(query, async(err, res) =>{
        if(err){
          reject(err)
          }else{
            resolve(JSON.parse(JSON.stringify(res))[0][table_field])
          }
        
        })
      })
    } else
  
  if(db_type == 'postgre'){
    const pgConn = new Client({
      host: host,
      user: user,
      port: port,
      password: password,
      database: database,
    })
  
    pgConn.connect()
  
    return new Promise((resolve, reject)=>{
      const query = `SELECT ${table_field} FROM ${table} ORDER BY ${table_field} DESC LIMIT 1`
      pgConn.query(query, (err, res)=>{
      if(err){
        // console.log(err);
        reject(err)
      }else{
        // console.log( JSON.parse(JSON.stringify(res))['rows'][0][table_field] );
        resolve(JSON.parse(JSON.stringify(res))['rows'][0][table_field])
      }
    })
  })
  }



}

function toUTCTime(unixData){ //Из юникстайма в время
  return ( (new Date(unixData * 1000)).toISOString() ).replace('T', ' ').split('.')[0]
}

function allTimeToMinutes(data){//И юникс и обычное время конвертит в минуты
    if(data == null) { return Math.floor( Date.now()/(1000*60) )}
    let total
    total = Math.floor( data / 60)
    if( isNaN(total) ){
      total = Math.floor( (Date.parse( data )) /inMinutes )
    }

    return total
}

async function checkBD(){
  console.log(' --------------------------');
  const baseList = await baseListPromice

  const allBDInfo = new Promise(async(resolve, reject)=>{
    let tgMessage = []
    for (let i = 0; i < baseList.length; i++) {
      const element = baseList[i];
      const {db_caption, db_host, db_port, db_type, db_name, db_user, db_password, table_name, table_field, deep_alert} = element
  
      await baseMonitor(db_host, db_user, db_password, db_name, db_port, table_name, table_field,db_type)
      .then( val =>{
        const dbUnixMinutes = allTimeToMinutes(val)
        const passTime = allTimeToMinutes() - dbUnixMinutes
  
        if( passTime < Number(deep_alert) ){
          console.log(`ОК --- ${ toUTCTime( dbUnixMinutes * 60 ) } ${db_caption}`);
          tgMessage.push(`${toUTCTime( dbUnixMinutes * 60)} ${db_caption}`)
        }else{
          console.log('!! ---', toUTCTime( dbUnixMinutes * 60), db_caption);
          tgMessage.push(`${toUTCTime( dbUnixMinutes * 60)} ${db_caption}`)
        }
        
      })
      .catch(()=>{
        console.log(`Ошбика в подключении к ${db_caption}`);
      })
    }

    resolve(tgMessage)
  })

  const tgMessage =( await allBDInfo ).toString().replace(',', '\n')

  if( tgMessage != [] ){
      try {
        bot.sendMessage(tg_group_id, await tgMessage )
          .then(()=>{
            process.exit()
          })
      } catch (error) {
        console.log('Группа недоступна');
      }
    }else{
      console.log('Нет');
    }


    

}
checkBD()