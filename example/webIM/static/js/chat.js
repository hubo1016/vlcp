$(function(){

    // here jquery will exec on ready event $()
    // console.log("jquery load");

    $("#messageform").on("submit",function(event){
        /*
        //form = ($(this)).serialize();
            serialize()  will serialize form to name=value&name=value
        //alert(form)
        formArray = $(this).serializeArray();
            serializeArray will serialize form to [{name:value},{name:value}]
        for(var i = 0; i < formArray.length; i++){
            //alert(formArray[i].name +'='+formArray[i].value);
        }

        if( $("#name").val().length == 0 || $('#name').val().length == 0){
             alert("name || password not NULL");
             //event.preventDefault();
        }else{

            $.post("/newmessage",{message:formArray[0].value},function(response){
                    console.log(response);
                    //updater.showmessage(response.id,response.message);
            });
            //return false;
        }
        */
        formArray = $(this).serializeArray();

        if ($("#name").val().length == 0){
            alert(" message is nil");
        }else{
            // the form only have one filed, so formArray[0]
            data = {message:formArray[0].value};
            $.post("/newmessage",data,function(response){
                // do nothing on response
                $("#name").val("");
            });
        }
        // here prevent browser from default form action

        event.preventDefault();
    });

    // ajax while(1)
    updater.poll();
});

var updater = {
    // index will store the last message id
    index : null,

    poll:function(){

        data = {index: updater.index};

        $.post("/updatemessage",data,function(response){
              // response : { messages : [ {id:id,message:message},{id:id,message:message} ] }
              var messageArray = response.messages;

              for(var i = 0; i < messageArray.length; i ++){

                    if(messageArray[i]){
                    //      console.log(messageArray[i].id);
                    //      console.log(messageArray[i].message);
                          updater.showmessage(messageArray[i].id,messageArray[i].message);
                    }
              }

              window.setTimeout(updater.poll,0);
        });
    },

    showmessage: function(id,message){
            updater.index = id;
            var newMessage = "<div>" + message + "</div>";
            $(newMessage).appendTo("#inbox");
    },
}