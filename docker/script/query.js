db.question.aggregate([
  {
    $match: {
      OwnerUserId: 26
    }
  }
])


  db.question.aggregate([
    {
      $match: {
        $or: [
          { Body: { $regex: /\b(Go|Golang)\b(?<comma>,)?/ } }  // Match "Golang" in Body
        ]
      }
    },
    {
      $addFields: {
        languageMentions: {
          $split: ["$Body", " "]
        }
      }
    },
    {
      $unwind: "$languageMentions"
    },
    {
        $match: {
          $and:[
            {languageMentions: { $regex: /\b(Go|Golang)\b(?<comma>,)?/ }},
            {languageMentions: { $not: {$regex: /\b(Go. | \;Go)\b/ }}}
          ]
        }
    },
    {
      $project: {
        _id : 1,
        Body:1,
        languageMentions: 1
      }
    }
  ])
  
  //chk
  db.question.aggregate([
    {
      $match: {
        $or: [
          { Body: { $regex: /\b(Golang)\b(?<comma>,)?/gi } }  // Match "Golang" in Body
        ]
      }
    },
    {
      $addFields: {
        languageMentions: {
          $split: ["$Body", " "]
        }
      }
    },
    {
      $unwind: "$languageMentions"
    },
    {
        $match: {
          languageMentions: { $regex: /\b(Golang)\b(?<comma>,)?/gi } 
        }
    },
    {
      $project: {
        _id : 1,
        languageMentions: 1
      }
    },
    {
      $group: {
        _id: null,
        count: { $sum: 1 }
      }
    }
  ])
  