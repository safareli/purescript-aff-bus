module Effect.Aff.CleanUp where

import Prelude

import Control.Monad.Reader (ReaderT, ask, lift, runReaderT)
import Data.Either (Either(..), either)
import Data.Exists (Exists, mkExists, runExists)
import Data.Foldable (for_)
import Data.Maybe (Maybe(..))
import Effect (Effect)
import Effect.Aff (Aff, Canceler(..), Error, generalBracket, makeAff)
import Effect.Class (liftEffect)
import Effect.Ref as Ref

data OpRes a
  = Resolved (Either Error a)
  | Killed Error

split :: forall a. OpRes a -> Either Error a
split (Resolved (Left err)) = Left err
split (Resolved (Right res)) = Right res
split (Killed err) = Left err

getErr :: forall a. OpRes a -> Maybe Error
getErr = split >>> either Just (const Nothing)

getRes :: forall res. OpRes res -> Maybe res
getRes = split >>> either (const Nothing) Just


type MakeAffCB a = ((Either Error a → Effect Unit) → Effect (Effect Unit))

hook'
  :: forall a
   . (OpRes a -> Effect Unit)
  -> (MakeAffCB a)
  -> Aff a
hook' cb f = makeAff \k -> do
  c ← f \res -> cb (Resolved res) *> k res
  pure $ Canceler \err -> liftEffect $ cb (Killed err) *> c

generalFinally
  :: forall res x
   . x
  -> ((x -> Effect Unit) -> Aff res)
  -> (OpRes res -> x -> Aff Unit)
  -> Aff res
generalFinally initialValue operation finally' =
  generalBracket (liftEffect $ Ref.new initialValue)
    { killed: finallyCB <<< Killed
    , failed: finallyCB <<< Resolved <<< Left
    , completed: finallyCB <<< Resolved <<< Right
    }
    (\ref -> operation $ flip Ref.write ref )
  where
    finallyCB res ref = do
      currentValue <- liftEffect $ Ref.read ref
      finally' res currentValue

type FinallyM res = ReaderT { registerCleanUp :: Exists CleanUpR -> Effect Unit } Aff res
type CleanUpM a res = ReaderT { current :: OpRes a, final :: Exists OpRes } Aff res

newtype CleanUpR a = CleanUpR
  { cleanUp :: CleanUpM a Unit
  , current :: OpRes a
  }

onResult :: forall a. (a -> CleanUpM a Unit) -> CleanUpM a Unit
onResult f = ask >>= \e -> for_ (getRes e.current) f

onError :: forall a. (Error -> CleanUpM a Unit) -> CleanUpM a Unit
onError f = ask >>= \e -> for_ (getErr e.current) f

onFinalError :: forall a. (Error -> CleanUpM a Unit) -> CleanUpM a Unit
onFinalError f = ask >>= \e -> for_ (runExists getErr e.final) f

hook
  :: forall a
   . CleanUpM a Unit
  -> MakeAffCB a
  -> FinallyM a
hook cleanUp f = do
  { registerCleanUp } <- ask
  lift $ hook' (\current -> registerCleanUp $ mkExists (CleanUpR {cleanUp, current}) ) f

runFinallyM
  :: forall a
   . FinallyM a
  -> Aff a
runFinallyM operation = generalFinally
  (Nothing)
  (\registerCleanUp -> runReaderT operation
      { registerCleanUp: registerCleanUp <<< Just
      }
  )
  (\res cleanUpEMb -> for_ cleanUpEMb
    $ runExists \(CleanUpR {cleanUp, current}) ->
        runReaderT cleanUp {current, final: mkExists res}
  )
