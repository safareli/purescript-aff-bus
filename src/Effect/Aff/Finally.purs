module Effect.Aff.Finally
  ( FinallyM
  , runFinallyM
  , DelayedFinalizer(..)

  , Finalizer
  , FinalizerEnv
  , onResult
  , onError
  , onFinalError
  , finally
  , finally'
  , finallyAff


  , MakeAffCB
  , affToMakeAffCB

  , Result(..)
  , isKilled
  , isFailed
  , isCompleted
  , split
  , getErr
  , getRes
  
  ) where

import Prelude

import Control.Monad.Reader (ReaderT, ask, lift, runReaderT)
import Data.Either (Either(..), either)
import Data.Exists (Exists, mkExists, runExists)
import Data.Foldable (for_)
import Data.Maybe (Maybe(..))
import Effect (Effect)
import Effect.Aff (Aff, Canceler(..), Error, effectCanceler, generalBracket, killFiber, makeAff, runAff)
import Effect.Class (liftEffect)
import Effect.Ref as Ref

-- | This type represents All things which can happen
-- | to `Aff a` see `generalBracket` and `BracketConditions`.
-- |
-- | * `Killed _` corresponds to `killed` case from `BracketConditions`
-- | * `Resolved (Left _)` corresponds to `failed` case from `BracketConditions`
-- | * `Resolved (Right _)` corresponds to `completed` case from `BracketConditions`
data Result a
  = Resolved (Either Error a)
  | Killed Error

isKilled :: forall a. Result a -> Boolean
isKilled (Killed _) = true
isKilled _ = false

isFailed :: forall a. Result a -> Boolean
isFailed (Resolved (Left _)) = true
isFailed _ = false

isCompleted :: forall a. Result a -> Boolean
isCompleted (Resolved (Right _)) = true
isCompleted _ = false

split :: forall a. Result a -> Either Error a
split (Resolved (Left err)) = Left err
split (Resolved (Right res)) = Right res
split (Killed err) = Left err

getErr :: forall a. Result a -> Maybe Error
getErr = split >>> either Just (const Nothing)

getRes :: forall a. Result a -> Maybe a
getRes = split >>> either (const Nothing) Just

-- Callback representation of Aff passed to `makeAff`
type MakeAffCB c a = ((Either Error a → Effect Unit) → Effect c)

-- | Convert any Aff to CallBak form.
-- | `affToMakeAffCB >>> makeAff :: Aff ~> Aff` is basically `identity`
affToMakeAffCB :: Aff ~> MakeAffCB Canceler
affToMakeAffCB aff = \k -> runAff k aff <#> flip killFiber >>> Canceler

-- | `Finalizer a` is basically an `Aff Unit` computation attached to some
-- | `Aff a`. besides doing arbitrary async effects, it has access to
-- | (FinalizerEnv a) using ReaderT, it allows observing results of the `Aff a`
-- | it's attached to, as well as observing results of the whole `FinallyM a`
-- | which the `Aff a` is executed in.
-- | `Finalizer a` is equivalent to `BracketConditions Unit a`
type Finalizer a = ReaderT (FinalizerEnv a) Aff Unit
type FinalizerEnv a = { current :: Result a, final :: Exists Result }

-- | If `Aff a` (the `Finalizer a` is attached to) succeeds,
-- | callback will be invoked with result.
onResult :: forall a. (a -> Finalizer a) -> Finalizer a
onResult f = ask >>= \e -> for_ (getRes e.current) f
-- | If `Aff a` (the `Finalizer a` is attached to) is killed or failed,
-- | callback will be invoked with error
onError :: forall a. (Error -> Finalizer a) -> Finalizer a
onError f = ask >>= \e -> for_ (getErr e.current) f

-- | Despite results of the `Aff a` (the `Finalizer a` is attached to)
-- | if the computation that Aff was part of fails, callback will be invoked with error
onFinalError :: forall a. (Error -> Finalizer a) -> Finalizer a
onFinalError f = ask >>= \e -> for_ (runExists getErr e.final) f

-- | `DelayedFinalizer a` stores `Result a` of `Aff a` to which the
-- | `Finalizer a` was attached to.
-- |
-- | NOTE: it's just implementation detail if constructor
-- | of FinallyM is not exposed this could be fully hidden in this module
newtype DelayedFinalizer a = DelayedFinalizer
  { finalizer :: Finalizer a
  , current :: Result a
  }

-- | `FinallyM a` is basically an `Aff a` computation which also allows
-- | storing of a DelayedFinalizer which will be executed as part of `runFinallyM`
type FinallyM a = ReaderT { setDelayedFinalizer :: Exists DelayedFinalizer -> Effect Unit } Aff a

-- | Registers a finalizer on a computation. which will be executed if:
-- | * the computation is killed (`pure true === asks _.current >>> isKilled` )
-- | * the computation fails (`pure true === asks _.current >>> isFailed`)
-- | * the computation succeeds and there is no more finalizer registered
-- | * the computation succeeds and an interrupt happens
-- |   up until any other computation which has registered finalizer. i.e.
-- |   if we have: 
-- |   ```
-- |   fib = launchAff $ runFinally do
-- |     // `killFiber err fib` can interrupt here, no finalizer is executed.
-- |     finallyAff aF a
-- |     // `killFiber err fib` can interrupt here, `aF` is executed where:
-- |     //   `ask >>= \e -> isCompleted e.current && isKilled e.final)`
-- |     lift b
-- |     // `killFiber err fib` can interrupt here, `aF` is executed where:
-- |     //   `ask >>= \e -> isCompleted e.current && isKilled e.final)`
-- |     finallyAff cF c
-- |     // `killFiber err fib` can interrupt here, `cF` is executed where:
-- |     //   `ask >>= \e -> isCompleted e.current && isKilled e.final)`
-- |   ```
-- |
-- | Relation with `invincible`:
-- | ```
-- | runFinally (finallyAff (onResult (f >>> lift)) (pure a)) === invincible (f a)
-- | runFinally (finallyAff (onError (f >>> lift)) (throwError e)) === invincible (f e)
-- | ```
finally
  :: forall a
   . Finalizer a
  -> MakeAffCB Canceler a
  -> FinallyM a
finally finalizer f = do
  { setDelayedFinalizer } <- ask
  lift $ makeAff $ preResultHook (\current -> setDelayedFinalizer $ mkExists (DelayedFinalizer {finalizer, current}) ) f
  where
    -- | Attache Effect handlers to different points of MakeAffCB.
    -- | Attached Effect handlers will be executed first
    preResultHook
      :: forall a'
      . (Result a' -> Effect Unit)
      -> MakeAffCB Canceler a'
      -> MakeAffCB Canceler a'
    preResultHook finalizer' f' = \k -> do
      canceler ← f' \res -> do
        finalizer' (Resolved res)
        k res
      pure
        $ (Canceler \err -> liftEffect $ finalizer' (Killed err))
        <> canceler

-- | Registers a finalizer on a computation. See `finally`.
finally'
  :: forall a
   . Finalizer a
  -> MakeAffCB (Effect Unit) a
  -> FinallyM a
finally' finalizer f = finally finalizer \k -> f k <#> \c -> effectCanceler c

-- | Registers a finalizer on a computation. See `finally`.
finallyAff
  :: forall a
   . Finalizer a
  -> Aff a
  -> FinallyM a
finallyAff finalizer f = finally finalizer $ affToMakeAffCB f

-- | Transforms FinallyM into Aff such that last registered finalizer is executed in the end.
-- | See finally
runFinallyM :: FinallyM ~> Aff
runFinallyM operation = generalBracket (liftEffect $ Ref.new Nothing)
    { killed: finallyCB <<< Killed
    , failed: finallyCB <<< Resolved <<< Left
    , completed: finallyCB <<< Resolved <<< Right
    }
    (\delayedFinalizerRef -> runReaderT operation
      { setDelayedFinalizer: flip Ref.write delayedFinalizerRef <<< Just
      }
    )
  where
    finallyCB operationResult delayedFinalizerRef = do
      delayedFinalizerExistsMaybe <- liftEffect $ Ref.read delayedFinalizerRef
      for_ delayedFinalizerExistsMaybe $ runExists \(DelayedFinalizer {finalizer, current}) ->
        runReaderT finalizer {current, final: mkExists operationResult}
